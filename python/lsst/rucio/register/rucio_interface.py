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

import logging
import zlib

import backoff
import requests
import rucio.common.exception
import urllib3.exceptions
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient

import lsst.daf.butler
from lsst.daf.butler import DatasetRef
from lsst.resources import ResourcePath
from lsst.rucio.register.resource_bundle import ResourceBundle
from lsst.rucio.register.rubin_meta import RubinMeta
from lsst.rucio.register.rucio_did import RucioDID

__all__ = ["RucioInterface"]

RETRYABLE = (
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.ProtocolError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    rucio.common.exception.ServerConnectionException,
    rucio.common.exception.DatabaseException,
)

logger = logging.getLogger(__name__)

_FACTOR = "factor"
_MAX_VALUE = "max_value"
_MAX_TRIES = "max_tries"
_BACKOFF = {_FACTOR: 5, _MAX_VALUE: 30, _MAX_TRIES: 5}


def _backoff_message(details):
    logger.info(
        "Backing off {wait:0.1f} seconds after {tries} tries "
        "calling function {target} with args {args} and kwargs "
        "{kwargs}".format(**details)
    )


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

    def compute_hashes(self, resource_path: ResourcePath) -> tuple[int, str]:
        """return the length and adler32 hash for a file.

        Parameters
        ----------
        path: `lsst.resources.ResourcePath`
            Path to the file.

        Returns
        -------
        hashes: `tuple` [ `int`, `str` ]
            Size in bytes and Adler32 hex hash.
        """

        info = resource_path.get_info()
        size = info.size
        checksums = info.checksums
        if "adler32" in checksums:
            adler32 = checksums["adler32"]
            logger.debug("found adler32 for %s", resource_path)
            return size, adler32
        return size, self._compute_adler32(resource_path)

    def _compute_adler32(self, resource_path: ResourcePath) -> tuple[int, str]:
        logger.debug("computing adler32 for %s", resource_path)
        adler32 = zlib.adler32(b"")
        buffer_size = 10 * 1024 * 1024
        with resource_path.open("rb") as f:
            while buffer := f.read(buffer_size):
                adler32 = zlib.adler32(buffer, adler32)
        adler32_digest = f"{adler32:08x}"
        return adler32_digest

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
            byte length, adler32 checksum, meta, and scope.
        """

        size, adler32 = self.compute_hashes(resource_path)
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
            name=name,
            scope=self.scope,
            meta=meta,
        )

        return d

    @backoff.on_exception(
        backoff.expo,
        RETRYABLE,
        factor=lambda: _BACKOFF[_FACTOR],
        max_value=lambda: _BACKOFF[_MAX_VALUE],
        max_tries=lambda: _BACKOFF[_MAX_TRIES],
        jitter=None,
        on_backoff=_backoff_message,
    )
    def _add_replicas(self, bundles: list[ResourceBundle]) -> None:
        """Call the Rucio method add_replica for a list of DIDs

        Parameters
        ----------
        bundles : `list` [`ResourceBundle`]
            A list of ResourceBundles
        """

        dids = [bundle.get_did() for bundle in bundles]

        self.replica_client.add_replicas(rse=self.rse, files=dids)

    @backoff.on_exception(
        backoff.expo,
        RETRYABLE,
        factor=lambda: _BACKOFF[_FACTOR],
        max_value=lambda: _BACKOFF[_MAX_VALUE],
        max_tries=lambda: _BACKOFF[_MAX_TRIES],
        jitter=None,
        on_backoff=_backoff_message,
    )
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
        try:
            self.did_client.add_files_to_datasets(
                attachments=[
                    {
                        "scope": self.scope,
                        "name": dataset_id,
                        "dids": dids,
                        "rse": self.rse,
                    }
                ],
                ignore_duplicate=True,
            )
            return
        except rucio.common.exception.DataIdentifierNotFound as e:
            raise e

    @backoff.on_exception(
        backoff.expo,
        RETRYABLE,
        factor=lambda: _BACKOFF[_FACTOR],
        max_value=lambda: _BACKOFF[_MAX_VALUE],
        max_tries=lambda: _BACKOFF[_MAX_TRIES],
        jitter=None,
        on_backoff=_backoff_message,
    )
    def _add_dataset_with_retries(self, dataset_id: str, statuses: dict) -> None:
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
                    logger.info("Couldn't register because dataset not yet registered")
                    logger.info("Creating Rucio dataset %s", dataset_id)
                    self._add_dataset_with_retries(
                        dataset_id=dataset_id,
                        statuses={"monotonic": True},
                    )
                except rucio.common.exception.DataIdentifierAlreadyExists:
                    # If someone else created it in the meantime
                    pass
                # And then retry adding DIDs
                logger.info("Dataset registered.")
                logger.info("Retrying registering %s in dataset %s, RSE %s", names, dataset_id, self.rse)
                self._add_files_to_dataset(dataset_id, dids)

        logger.debug("Done with Rucio for %s", bundles)

    def set_backoff(self, factor, max_value, max_tries) -> None:
        """Set backoff values for retries

        Parameters
        ----------
        factor: `float`
            Multipler for backoff
        max_value: `int`
            Maximum seconds to backoff to
        max_tries: `int`
            Maximum times to try
        """
        logger.debug("factor=%f, max_value=%d, max_tries=%d", factor, max_value, max_tries)
        _BACKOFF[_FACTOR] = factor
        _BACKOFF[_MAX_VALUE] = max_value
        _BACKOFF[_MAX_TRIES] = max_tries

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
