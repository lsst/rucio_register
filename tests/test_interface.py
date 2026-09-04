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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch

import urllib3
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.common.exception import (
    DataIdentifierNotFound,
    FileAlreadyExists,
    RucioException,
)

import lsst.utils.tests
from lsst.daf.butler import Butler, DatasetRef, DimensionUniverse
from lsst.resources import ResourceInfo, ResourcePath
from lsst.resources.file import FileResourcePath
from lsst.rucio.register.data_type import DataType
from lsst.rucio.register.rucio_interface import RucioInterface


class InterfaceTestCase(lsst.utils.tests.TestCase):
    maxDiff = None
    FAST_BACKOFF = {"factor": 0.001, "max_value": 0.01, "max_tries": 2}

    def setUp(self):
        self.butler_repo = tempfile.mkdtemp(dir="/tmp")
        test_dir = os.path.abspath(os.path.dirname(__file__))

        self.dataset_ref_file = os.path.join(test_dir, "data", "dataset_ref.json")

        Butler.makeRepo(self.butler_repo)

        data_name = "visitSummary_HSC_y_HSC-Y_318_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.fits"
        json_name = "visitSummary_HSC_y_HSC-Y_318_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.json"

        self.data_file = os.path.join(test_dir, "data", data_name)
        self.json_file = os.path.join(test_dir, "data", json_name)

        self.butler = Butler(self.butler_repo, writeable=True)
        self.butler.getURI = MagicMock(return_value=ResourcePath(f"file://{self.data_file}"))

        self.rse_root = tempfile.mkdtemp(dir="/tmp")

        # patch __init__ methods
        self.rc_init = patch.object(ReplicaClient, "__init__", return_value=None)
        self.dc_init = patch.object(DIDClient, "__init__", return_value=None)
        self.rc_add_replicas = patch.object(ReplicaClient, "add_replicas", return_value=None)
        self.dc_attach_dids = patch.object(DIDClient, "attach_dids", return_value=None)
        self.dc_attach_dids_to_dids = patch.object(DIDClient, "attach_dids_to_dids", return_value=None)
        self.dc_add_dataset = patch.object(DIDClient, "add_dataset", return_value=None)
        self.rand = patch("random.randint", return_value=1)

        self.mock_rc_init = self.rc_init.start()
        self.mock_dc_init = self.dc_init.start()
        self.mock_rc_add_replicas = self.rc_add_replicas.start()
        self.mock_dc_attach_dids = self.dc_attach_dids.start()
        self.mock_dc_attach_dids_to_dids = self.dc_attach_dids_to_dids.start()
        self.mock_dc_add_dataset = self.dc_add_dataset.start()
        self.mock_rand = self.rand.start()

        rucio_rse = "DRR1"
        scope = "test"
        dtn_url = "root://xrd1:1094//rucio"
        self.ri = RucioInterface(self.butler, rucio_rse, scope, self.rse_root, dtn_url, DataType.DATA_PRODUCT)
        self.rpath = ResourcePath(f"{self.data_file}")
        self.fpath = FileResourcePath(f"{self.data_file}")

    def testResourcePathCase(self):
        res = self.ri.compute_hashes(self.rpath)
        self.assertEqual(res, (1365120, "480be4de"))

    def testFileResourcePathCase(self):
        res = self.ri.compute_hashes(self.fpath)
        self.assertEqual(res, (1365120, "480be4de"))

    def testChecksumsCase(self):
        fake_info = MagicMock(spec=ResourceInfo)
        fake_info.checksums = {"adler32": "abcd1234"}
        fake_info.size = 1234
        fake_info.is_file = True
        fake_info.last_modified = None
        fake_info.uri = f"file://{self.data_file}"

        patcher = patch.object(FileResourcePath, "get_info", return_value=fake_info)
        patcher.start()
        self.addCleanup(patcher.stop)

        with open(self.dataset_ref_file) as f:
            json_ref = f.readline()

        ref = DatasetRef.from_json(json_ref, DimensionUniverse())

        self.butler.registry.registerDatasetType(ref.datasetType)
        cnt = self.ri.register_as_replicas("mydataset", [ref])
        self.assertEqual(cnt, 1)

        rb = self.ri._make_dataset_ref_bundle("mydataset", ref)
        self.assertEqual(rb.dataset_id, "mydataset")

        did = rb.did.model_dump()
        self.assertEqual(did["adler32"], "abcd1234")

    def testHashCacheReusesComputedChecksum(self):
        fake_path = MagicMock()
        fake_path.scheme = "custom"
        fake_path.__str__.return_value = "custom://repo/file.fits"
        fake_path.get_info.return_value = MagicMock(size=1234, checksums={"adler32": "12345678"})

        res1 = self.ri.compute_hashes(fake_path)
        self.assertEqual(res1, (1234, "12345678"))
        self.assertEqual(self.ri._checksum_sources["standard WebDAV metadata"], 1)

        res2 = self.ri.compute_hashes(fake_path)
        self.assertEqual(res2, (1234, "12345678"))
        self.assertEqual(self.ri._checksum_sources["internal hash cache"], 1)

    def testInterfaceTestCase(self):
        dtn_url = "root://xrd1:1094//rucio"

        json_ref = None
        with open(self.dataset_ref_file) as f:
            json_ref = f.readline()

        ref = DatasetRef.from_json(json_ref, DimensionUniverse())

        self.butler.registry.registerDatasetType(ref.datasetType)
        cnt = self.ri.register_as_replicas("mydataset", [ref])
        self.assertEqual(cnt, 1)

        rb = self.ri._make_dataset_ref_bundle("mydataset", ref)
        self.assertEqual(rb.dataset_id, "mydataset")

        did = rb.did.model_dump()
        self.assertEqual(did["pfn"], f"{dtn_url}{self.data_file}")
        self.assertEqual(did["bytes"], 1365120)
        self.assertEqual(did["adler32"], "480be4de")
        self.assertEqual(did["name"], self.data_file)
        self.assertEqual(did["scope"], "test")

        meta = did["meta"]
        self.assertEqual(meta["rubin_butler"], DataType.DATA_PRODUCT)

    def common(self):
        json_ref = None
        with open(self.dataset_ref_file) as f:
            json_ref = f.readline()

        ref = DatasetRef.from_json(json_ref, DimensionUniverse())

        self.butler.registry.registerDatasetType(ref.datasetType)
        self.ri.register_as_replicas("mydataset", [ref])

    @patch.dict("lsst.rucio.register.rucio_interface._BACKOFF", FAST_BACKOFF)
    @patch.object(ReplicaClient, "add_replicas", side_effect=RucioException("failed"))
    def testException1TestCase(self, MC1):
        self.ri.register_to_dataset = MagicMock(name="register_to_dataset")
        with self.assertRaises(Exception):
            self.common()

    @patch.dict("lsst.rucio.register.rucio_interface._BACKOFF", FAST_BACKOFF)
    @patch.object(DIDClient, "add_files_to_dataset", side_effect=FileAlreadyExists("failed"))
    def testException2TestCase(self, MC1):
        self.common()

    def testClearIsNewMetadata(self):
        self.ri.clear_is_new = True
        self.ri.did_client = MagicMock()
        self.ri.did_client.set_metadata = MagicMock(name="set_metadata")

        fake_bundle = MagicMock()
        fake_bundle.dataset_id = "dataset_001"
        fake_bundle.get_did.return_value = {"pfn": "test_pfn"}

        with patch.object(self.ri, "_add_files_to_dataset"):
            self.ri.register_to_dataset([fake_bundle])

        self.ri.did_client.set_metadata.assert_called_once_with(
            scope="test", name="dataset_001", key="is_new", value=None
        )

    def testRetriesConfigured(self):
        self.ri.set_backoff(factor=2.0, max_value=60, max_tries=12)
        self.assertEqual(self.ri.retries, 12)

    @patch.dict("lsst.rucio.register.rucio_interface._BACKOFF", {"factor": 0.5, "max_tries": 3})
    @patch.object(
        ReplicaClient,
        "add_replicas",
        side_effect=urllib3.exceptions.ReadTimeoutError(pool=None, url="http://127.0.0.1/", message="failed"),
    )
    def testException3TestCase(self, MC3):
        self.ri.register_to_dataset = MagicMock(name="register_to_dataset")
        with self.assertRaises(Exception):
            self.common()

    @patch.dict("lsst.rucio.register.rucio_interface._BACKOFF", {"factor": 0.5, "max_tries": 3})
    @patch.object(
        DIDClient,
        "add_files_to_datasets",
        side_effect=urllib3.exceptions.ReadTimeoutError(pool=None, url="http://127.0.0.1/", message="failed"),
    )
    def testException4TestCase(self, MC4):
        with self.assertRaises(Exception):
            self.common()

    @patch.dict("lsst.rucio.register.rucio_interface._BACKOFF", FAST_BACKOFF)
    @patch.object(DIDClient, "add_files_to_datasets", side_effect=DataIdentifierNotFound("failed"))
    def testException5TestCase(self, MC5):
        with self.assertRaises(Exception):
            self.common()

    @patch.dict("lsst.rucio.register.rucio_interface._BACKOFF", {"factor": 0.5, "max_tries": 3})
    @patch.object(DIDClient, "add_files_to_datasets", side_effect=DataIdentifierNotFound("failed"))
    @patch.object(
        DIDClient,
        "add_dataset",
        side_effect=urllib3.exceptions.ReadTimeoutError(pool=None, url="http://127.0.0.1/", message="failed"),
    )
    def testException6TestCase(self, MC6a, MC6b):
        with self.assertRaises(Exception):
            self.common()

    def testMultiThreadedRegisterAsReplicas(self):
        json_ref = None
        with open(self.dataset_ref_file) as f:
            json_ref = f.readline()

        ref = DatasetRef.from_json(json_ref, DimensionUniverse())
        self.butler.registry.registerDatasetType(ref.datasetType)

        refs = [ref] * 25
        cnt = self.ri.register_as_replicas("multithread_dataset", refs)
        self.assertEqual(cnt, 25)
        self.mock_rc_add_replicas.assert_called()

    def tearDown(self):
        patch.stopall()
        shutil.rmtree(self.butler_repo, ignore_errors=True)
        shutil.rmtree(self.rse_root, ignore_errors=True)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
