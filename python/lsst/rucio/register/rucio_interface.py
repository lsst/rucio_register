# This file is part of rucio_register
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

import rucio.common.exception
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient

import lsst.daf.butler
from lsst.daf.butler import DatasetRef
from lsst.resources import ResourcePath
from lsst.rucio.register.resource_bundle import ResourceBundle
from lsst.rucio.register.rubin_meta import RubinMeta
from lsst.rucio.register.rucio_did import RucioDID

__all__ = ["RucioInterface"]

logger = logging.getLogger(__name__)


class RucioInterface:
    """Add files as replicas in Rucio, along with metadata,
    and attach them to datasets.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler we're operating upon
    rucio_rse : `str`
        Name of the RSE that the files live in.
    scope : `str`
        Rucio scope to register the files in.
    rse_root : `str`
        Full path to root directory of RSE directory structure
    dtn_url : `str`
        Base URL of the data transfer node for the Rucio physical filename.
    rubin_butler_type: `str`
        the type registered in "rubin_butler" metadata for rucio
    """

    def __init__(
        self,
        butler: lsst.daf.butler.Butler,
        rucio_rse: str,
        scope: str,
        rse_root: str,
        dtn_url: str,
        rubin_butler_type: str,
    ):
        self.butler = butler
        self.rse = rucio_rse
        self.scope = scope
        self.rse_root = rse_root
        self.dtn_url = dtn_url
        self.pfn_base = f"{dtn_url}"
        self.replica_client = ReplicaClient()
        self.did_client = DIDClient()
        self.rubin_butler_type = rubin_butler_type

    def _make_dataset_ref_bundle(self, dataset_id: str, dataset_ref: DatasetRef) -> ResourceBundle:
        """Make a ResourceBundle

        Parameters
        ----------
        dataset_id : `str`
            Rucio dataset name
        dataset_ref : `DatasetRef`
            Butler DatasetRef

        Returns
        -------
        rb : `ResourceBundle`
            ResourceBundle consolidating dataset id and DatasetRef
        """
        logging.debug("%s", dataset_ref.to_json())
        did = self._make_did(self.butler.getURI(dataset_ref), dataset_ref.to_json())
        rb = ResourceBundle(dataset_id=dataset_id, did=did)
        return rb

    def _make_zip_bundle(self, dataset_id: str, resource_path: ResourcePath) -> ResourceBundle:
        """Make a ResourceBundle

        Parameters
        ----------
        dataset_id : `str`
            Rucio dataset name
        resouce_path : `ResourcePath`
            ResourcePath to a file

        Returns
        -------
        rb: ResourceBundle
            ResourceBundle consolidating dataset id and ResourcePath
        """
        did = self._make_did(resource_path)
        rb = ResourceBundle(dataset_id=dataset_id, did=did)
        return rb

    def _make_dim_bundle(self, dataset_id: str, resource_path: ResourcePath) -> ResourceBundle:
        """Make a ResourceBundle

        Parameters
        ----------
        dataset_id : `str`
            Rucio dataset name
        resouce_path : `lsst.resource.ResourcePath`
            ResourcePath to a file

        Returns
        -------
        rb: `lsst.rucio.register.rucio_bundle.ResourceBundle`
            ResourceBundle consolidating dataset id and ResourcePath
        """
        did = self._make_did(resource_path)
        rb = ResourceBundle(dataset_id=dataset_id, did=did)
        return rb

    def compute_hashes(self, resource_path: ResourcePath) -> tuple[int, str, str]:
        """Compute the length, MD5, and Adler32 hashes for a file.

        Parameters
        ----------
        path: `lsst.resources.ResourcePath`
            Path to the file.

        Returns
        -------
        hashes: `tuple` [ `int`, `str`, `str` ]
            Size in bytes, MD5 hex, and Adler32 hex hashes.
        """
        size = 0
        md5 = hashlib.md5()
        adler32 = zlib.adler32(b"")
        buffer_size = 10 * 1024 * 1024
        with resource_path.open("rb") as f:
            while buffer := f.read(buffer_size):
                size += len(buffer)
                md5.update(buffer)
                adler32 = zlib.adler32(buffer, adler32)
        md5_digest = md5.hexdigest()
        adler32_digest = f"{adler32:08x}"
        return (size, md5_digest, adler32_digest)

    def _make_did(self, resource_path: ResourcePath, metadata: str = None) -> RucioDID:
        """Make a Rucio data identifier dictionary from a resource.

        Parameters
        ----------
        resource_path: ResourcePath
            ResourcePath object

        metadata: `str`
            String containing Rubin dataset specific metadata

        Returns
        -------
        did : `dict` [`str`, `str`|`int`]
            Rucio data identifier including physical and logical names,
            byte length, adler32 and MD5 checksums, meta, and scope.
        """

        size, md5, adler32 = self.compute_hashes(resource_path)
        path = resource_path.unquoted_path.removeprefix(self.rse_root)
        pfn = self.pfn_base + path
        logging.debug("pfn=%s", pfn)
        name = path.removeprefix("/" + self.scope + "/")
        logging.debug("name=%s", name)
        logging.debug("path=%s", path)

        if metadata:
            meta = RubinMeta(rubin_butler=self.rubin_butler_type, rubin_sidecar=metadata)
        else:
            meta = RubinMeta(rubin_butler=self.rubin_butler_type, rubin_sidecar="")
        d = RucioDID(
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
        bundles : `list` [`ResourceBundle`]
            A list of ResourceBundles
        """
        dids = [bundle.get_did() for bundle in bundles]
        retries = 0
        max_retries = 5
        while True:
            try:
                self.replica_client.add_replicas(rse=self.rse, files=dids)
                break
            except rucio.common.exception.RucioException:
                retries += 1
                if retries < max_retries:
                    seconds = random.randint(10, 20)
                    logger.debug("failed to add_replicas; sleeping %d seconds", seconds)
                    time.sleep(seconds)
                    self.replica_client = ReplicaClient()  # XXX not sure we need to do this.
                else:
                    raise Exception(f"Tried {max_retries} times and couldn't add_replicas")

    def _add_file_to_dataset_with_retries(self, dataset_id, did):
        retries = 0
        max_retries = 5
        while True:
            try:
                self.did_client.add_files_to_dataset(
                    scope=self.scope, name=dataset_id, files=[did], rse=self.rse
                )
                break
            except rucio.common.exception.FileAlreadyExists:
                if "pfn" in did:
                    logger.debug("file %s already registered in dataset %s", did["pfn"], dataset_id)
                return  # we can return, because it's already in the dataset
            except rucio.common.exception.RucioException:
                retries += 1
                if retries < max_retries:
                    seconds = random.randint(10, 20)
                    logger.debug("failed to register one did to %s; sleeping %d seconds", dataset_id, seconds)
                    time.sleep(seconds)
                    self.did_client = DIDClient()  # XXX not sure we need to do this.
                else:
                    # we tried max_retries times, and failed, so we'll bail out
                    raise Exception(f"Couldn't add {did['pfn']} to dataset {dataset_id}")

    def _add_files_to_dataset(self, dataset_id: str, dids: list[dict]) -> None:
        """Attach a list of files specified by Rucio DIDs to a Rucio dataset.

        Ignores already-attached files for idempotency.

        Parameters
        ----------
        dataset_id : `str`
            Logical name of the Rucio dataset.
        dids : `list` [`dict` [`str`, `str`|`int`] ]
            List of Rucio data identifiers.
        """
        retries = 0
        max_retries = 5
        while True:
            try:
                self.did_client.add_files_to_dataset(
                    scope=self.scope,
                    name=dataset_id,
                    files=dids,
                    rse=self.rse,
                )
                return
            except rucio.common.exception.FileAlreadyExists:
                # At least one already is in the dataset.
                # This shouldn't happen, but if it does,
                # we have to retry each individually.
                for did in dids:
                    self._add_file_to_dataset_with_retries(
                        dataset_id=dataset_id,
                        did=did,
                    )
                return
            except rucio.common.exception.DataIdentifierNotFound as e:
                raise e
            except rucio.common.exception.RucioException:
                retries += 1
                if retries < max_retries:
                    seconds = random.randint(10, 20)
                    logger.debug("failed to register dids to %s; sleeping %d", dataset_id, seconds)
                    time.sleep(seconds)
                    continue
                else:
                    raise Exception(f"Couldn't add files to dataset {dataset_id}")

    def _add_dataset_with_retries(self, dataset_id: str, statuses: dict) -> None:
        retries = 0
        max_retries = 5
        while True:
            try:
                self.did_client.add_dataset(
                    scope=self.scope,
                    name=dataset_id,
                    statuses=statuses,
                    rse=self.rse,
                )
                return
            except rucio.common.exception.DataIdentifierAlreadyExists as e:
                # If someone else created it in the meantime
                raise e
            except rucio.common.exception.RucioException:
                retries += 1
                if retries < max_retries:
                    seconds = random.randint(10, 20)
                    logger.debug("couldn't register dids to %s; waiting %d", dataset_id, seconds)
                    time.sleep(seconds)
                    continue
                else:
                    raise Exception(f"Tried {max_retries} times and couldn't add dataset {dataset_id}")

    def register_to_dataset(self, bundles) -> None:
        """Register a list of files in Rucio.

        Parameters
        ----------
        bundles : `list` [`ResourceBundle`]
            List of resource bundles
        """
        logger.debug("register to dataset")

        datasets = dict()
        for bundle in bundles:
            dataset_id = bundle.dataset_id
            datasets.setdefault(dataset_id, []).append(bundle)

        for dataset_id, bundles in datasets.items():
            try:
                dids = [rb.get_did() for rb in bundles]
                names = [did["pfn"] for did in dids]
                logger.info("Registering %s in dataset %s, RSE %s", names, dataset_id, self.rse)
                self._add_files_to_dataset(dataset_id, dids)
            except rucio.common.exception.DataIdentifierNotFound:
                # No such dataset, so create it
                try:
                    logger.info("Creating Rucio dataset %s", dataset_id)
                    self._add_dataset_with_retries(
                        dataset_id=dataset_id,
                        statuses={"monotonic": True},
                    )
                except rucio.common.exception.DataIdentifierAlreadyExists:
                    # If someone else created it in the meantime
                    pass
                # And then retry adding DIDs
                self._add_files_to_dataset(dataset_id, dids)

        logger.debug("Done with Rucio for %s", bundles)

    def register_as_replicas(self, dataset_id, dataset_refs) -> None:
        """Register a list of DatasetRefs to a Rucio dataset

        Parameters
        ----------
        dataset_id : `str`
            RUCIO dataset id
        dataset_refs : `list` [`DatasetRef`]
            list of Butler DatasetRefs
        """
        bundles = []
        for dataset_ref in dataset_refs:
            if type(dataset_ref) is list:
                for dsr in dataset_ref:
                    bundles.append(self._make_dataset_ref_bundle(dataset_id, dsr))
            else:
                bundles.append(self._make_dataset_ref_bundle(dataset_id, dataset_ref))
        if len(bundles) == 0:
            return 0
        self._add_replicas(bundles)
        self.register_to_dataset(bundles)
        return len(bundles)

    def register_zips(self, dataset_id: str, zip_files: list) -> int:
        """Register a list of zips to a Rucio Dataset

        Parameters
        ----------
        dataset_id : `str`
            RUCIO dataset id
        zip_files : `list` [`ResourcePath`]
            list of ResourcePath

        Returns
        -------
        num : `int`
            number of zip files ingested
        """
        bundles = []
        for zip_file in zip_files:
            bundles.append(self._make_zip_bundle(dataset_id, zip_file))
        self._add_replicas(bundles)
        self.register_to_dataset(bundles)
        return len(bundles)

    def register_dims(self, dataset_id: str, dim_files: list) -> int:
        """Register a list of dimension files to a Rucio Dataset

        Parameters
        ----------
        dataset_id : `str`
            RUCIO dataset id
        dim_files : `list` [`lsst.resource.ResourcePath`]
            list of ResourcePath

        Returns
        -------
        num : `int`
            number of dimension files ingested
        """
        bundles = []
        for dim_file in dim_files:
            bundles.append(self._make_dim_bundle(dataset_id, dim_file))
        self._add_replicas(bundles)
        self.register_to_dataset(bundles)
        return len(bundles)
