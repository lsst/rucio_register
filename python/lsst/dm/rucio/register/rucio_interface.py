# This file is part of dm_rucio_register
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import hashlib
import logging
import random
import time
import zlib

import lsst.daf.butler
import rucio.common.exception
from lsst.dm.rucio.register.resource_bundle import ResourceBundle
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient

__all__ = ["RucioInterface"]

logger = logging.getLogger(__name__)


class RucioInterface:
    """Add files as replicas in Rucio, along with metadata,
    and attach them to datasets.

    Parameters
    ----------
    butler: `lsst.daf.butler.Butler`
        Butler we're operating upon
    rucio_rse: `str`
        Name of the RSE that the files live in.
    scope: `str`
        Rucio scope to register the files in.
    rse_root: `str`
        Full path to root directory of RSE directory structure
    dtn_url: `str`
        Base URL of the data transfer node for the Rucio physical filename.
    """

    def __init__(
        self,
        butler: lsst.daf.butler.Butler,
        rucio_rse: str,
        scope: str,
        rse_root: str,
        dtn_url: str,
    ):
        self.butler = butler
        self.rse = rucio_rse
        self.scope = scope
        self.rse_root = rse_root
        self.dtn_url = dtn_url
        self.pfn_base = f"{dtn_url}"

    def _make_bundle(self, dataset_id, dataset_ref) -> ResourceBundle:
        """Make a ResourceBundle

        Parameters
        ----------
        dataset_id: `str`
            Rucio dataset name
        dataset_ref: `DatasetRef`
            Butler DatasetRef
        """
        did = self._make_did(self.butler.getURI(dataset_ref), dataset_ref.to_json())
        rb = ResourceBundle(dataset_id, did)
        return rb

    def _make_did(self, resource_path, metadata: str) -> dict[str, str | int]:
        """Make a Rucio data identifier dictionary from a resource.

        Parameters
        ----------
        resource_path: ResourcePath
            ResourcePath object

        metadata: `str`
            String containing Rubin dataset specific metadata

        Returns
        -------
        did: `dict [ str, str|int ]`
            Rucio data identifier including physical and logical names,
            byte length, adler32 and MD5 checksums, meta, and scope.
        """
        logging.info(f"type(resource_path) = {type(resource_path)}")
        with resource_path.open("rb") as f:
            contents = f.read()
            size = len(contents)
            md5 = hashlib.md5(contents).hexdigest()
            adler32 = f"{zlib.adler32(contents):08x}"
        path = resource_path.path.removeprefix(self.rse_root)
        pfn = self.pfn_base + path
        logging.debug(f"pfn = {pfn}")
        name = path.removeprefix("/" + self.scope + "/")

        meta = {"rubin_butler": 1, "rubin_sidecar": metadata}
        d = dict(
            pfn=pfn,
            bytes=size,
            adler32=adler32,
            md5=md5,
            name=name,
            scope=self.scope,
            meta=meta,
        )

        return d

    def _add_replicas(self, bundles: list[ResourceBundle]) -> None:
        """Call the Rucio method add_replica for a list of DIDs

        Parameters
        ----------
        bundles: `list[ResourceBundle]`
            A list of ResourceBundles
        """
        logger.info(f"rse={self.rse}, bundles={bundles}")
        dids = [bundle.get_did() for bundle in bundles]
        logger.info(f"{dids[0]=}")
        self.replica_client = ReplicaClient()
        self.replica_client.add_replicas(rse=self.rse, files=dids)

    def _add_files_to_dataset(self, did_client, dataset_id: str, dids: list[dict]) -> None:
        """Attach a list of files specified by Rucio DIDs to a Rucio dataset.

        Ignores already-attached files for idempotency.

        Parameters
        ----------
        dataset_id: `str`
            Logical name of the Rucio dataset.
        dids: `list [ dict [ str, str|int ] ]`
            List of Rucio data identifiers.
        """
        retries = 0
        max_retries = 2
        while True:
            try:
                did_client.add_files_to_dataset(
                    scope=self.scope,
                    name=dataset_id,
                    files=dids,
                    rse=self.rse,
                )
                return
            except rucio.common.exception.FileAlreadyExists:
                logger.info(f"FileAlreadyExists 1, dataset_id = {dataset_id}")
                # At least one already is in the dataset.
                # This shouldn't happen, but if it does,
                # we have to retry each individually.
                for did in dids:
                    try:
                        did_client.add_files_to_dataset(
                            scope=self.scope,
                            name=dataset_id,
                            files=[did],
                            rse=self.rse,
                        )
                    except rucio.common.exception.FileAlreadyExists:
                        logger.info("FileAlreadyExists 2")
                        pass
                return
            except rucio.common.exception.DatabaseException:
                retries += 1
                if retries < max_retries:
                    time.sleep(random.uniform(0.5, 2))
                    continue
                else:
                    raise

    def register_to_dataset(self, bundles) -> None:
        """Register a list of files in Rucio.

        Parameters
        ----------
        Bundles: `list [ ResourceBundle ]`
            List of resource bundles
        """
        logger.debug("register to dataset")

        did_client = DIDClient()

        datasets = dict()
        for bundle in bundles:
            dataset_id = bundle.get_dataset_id()
            datasets.setdefault(dataset_id, []).append(bundle)

        for dataset_id, bundles in datasets.items():
            try:
                dids = []
                for rb in bundles:
                    dids.append(rb.get_did())
                logger.info("Registering %s in dataset %s, RSE %s", dids, dataset_id, self.rse)
                self._add_files_to_dataset(did_client, dataset_id, dids)
            except rucio.common.exception.DataIdentifierNotFound:
                # No such dataset, so create it
                try:
                    logger.info("Creating Rucio dataset %s", dataset_id)
                    did_client.add_dataset(
                        scope=self.scope,
                        name=dataset_id,
                        statuses={"monotonic": True},
                        rse=self.rse,
                    )
                except rucio.common.exception.DataIdentifierAlreadyExists:
                    # If someone else created it in the meantime
                    logger.info("DataIndentifierAlreadyExists")
                    pass
                # And then retry adding DIDs
                self._add_files_to_dataset(did_client, dataset_id, dids)

        logger.debug("Done with Rucio for %s", bundles)

    def register_as_replicas(self, dataset_id, dataset_refs) -> None:
        """Register a list of DatasetRefs to a Rucio dataset

        Parameters
        ----------
        dataset_id: `str`
            RUCIO dataset id
        dataset_refs: `list[DatasetRef]`
            list of Butler DatasetRefs
        """
        bundles = []
        for dataset_ref in dataset_refs:
            bundles.append(self._make_bundle(dataset_id, dataset_ref))
        if len(bundles) == 0:
            return 0
        self._add_replicas(bundles)
        self.register_to_dataset(bundles)
        return len(bundles)