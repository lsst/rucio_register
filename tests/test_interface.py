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

from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.common.exception import (
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    FileAlreadyExists,
    RucioException,
)

import lsst.utils.tests
from lsst.daf.butler import Butler, DatasetRef, DimensionUniverse

# from lsst.daf.butler.registry import DatasetTypeError, MissingCollectionError
from lsst.resources import ResourcePath
from lsst.rucio.register.data_type import DataType
from lsst.rucio.register.rucio_interface import RucioInterface


class InterfaceTestCase(lsst.utils.tests.TestCase):
    maxDiff = None

    def setUp(self):
        self.butler_repo = tempfile.mkdtemp()
        test_dir = os.path.abspath(os.path.dirname(__file__))

        self.dataset_ref_file = os.path.join(test_dir, "data", "dataset_ref.json")

        Butler.makeRepo(self.butler_repo)

        data_name = "visitSummary_HSC_y_HSC-Y_318_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.fits"
        json_name = "visitSummary_HSC_y_HSC-Y_318_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.json"

        self.data_file = os.path.join(test_dir, "data", data_name)
        self.json_file = os.path.join(test_dir, "data", json_name)

        self.butler = Butler(self.butler_repo, writeable=True)
        self.butler.getURI = MagicMock(return_value=ResourcePath(f"file://{self.data_file}"))

        self.rse_root = tempfile.mkdtemp()

        # patch __init__ methods
        self.rc_init = patch.object(ReplicaClient, "__init__", return_value=None)
        self.dc_init = patch.object(DIDClient, "__init__", return_value=None)
        self.rc_add_replicas = patch.object(ReplicaClient, "add_replicas", return_value=None)
        self.dc_attach_dids = patch.object(DIDClient, "attach_dids", return_value=None)
        self.rand = patch("random.randint", return_value=1)

        self.mock_rc_init = self.rc_init.start()
        self.mock_dc_init = self.dc_init.start()
        self.mock_rc_add_replicas = self.rc_add_replicas.start()
        self.mock_dc_attach_dids = self.dc_attach_dids.start()
        self.mock_rand = self.rand.start()

        rucio_rse = "DRR1"
        scope = "test"
        dtn_url = "root://xrd1:1094//rucio"
        self.ri = RucioInterface(self.butler, rucio_rse, scope, self.rse_root, dtn_url, DataType.DATA_PRODUCT)

    def testInterfaceTestCase(self):
        dtn_url = "root://xrd1:1094//rucio"

        json_ref = None
        with open(self.dataset_ref_file) as f:
            json_ref = f.readline()

        ref = DatasetRef.from_json(json_ref, DimensionUniverse())

        self.butler.registry.registerDatasetType(ref.datasetType)
        cnt = self.ri.register_as_replicas("mydataset", [ref])
        self.assertEqual(cnt, 1)

        rb = self.ri._make_bundle("mydataset", ref)
        self.assertEqual(rb.dataset_id, "mydataset")

        did = rb.did.model_dump()
        self.assertEqual(did["pfn"], f"{dtn_url}{self.data_file}")
        self.assertEqual(did["bytes"], 1365120)
        self.assertEqual(did["adler32"], "480be4de")
        self.assertEqual(did["md5"], "a7ee5c19f5717bcf8d772de202864244")
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

    @patch.object(ReplicaClient, "add_replicas", side_effect=RucioException("failed"))
    def testException1TestCase(self, MC1):
        self.ri.register_to_dataset = MagicMock(name="register_to_dataset")
        with self.assertRaises(Exception):
            self.common()

    @patch.object(DIDClient, "add_files_to_dataset", side_effect=RucioException("failed"))
    def testException2TestCase(self, MC1):
        with self.assertRaises(Exception):
            self.common()

    @patch.object(DIDClient, "add_files_to_dataset", side_effect=FileAlreadyExists("failed"))
    def testException3TestCase(self, MC1):
        self.common()

    @patch.object(DIDClient, "add_dataset", return_value=None)
    @patch.object(DIDClient, "add_files_to_dataset", side_effect=DataIdentifierNotFound("failed"))
    def testException4TestCase(self, MC1, MC2):
        with self.assertRaises(Exception):
            self.common()

    @patch.object(DIDClient, "add_files_to_dataset", side_effect=RucioException("failed"))
    def testException5TestCase(self, MC1):
        with self.assertRaises(Exception):
            self.common()

    @patch.object(DIDClient, "add_dataset", side_effect=DataIdentifierAlreadyExists("failed"))
    @patch.object(DIDClient, "add_files_to_dataset", side_effect=DataIdentifierNotFound("failed"))
    def testException6TestCase(self, MC1, MC2):
        with self.assertRaises(Exception):
            self.common()

    @patch.object(DIDClient, "add_files_to_dataset", side_effect=RucioException("failed"))
    def testException7TestCase(self, MC1):
        with self.assertRaises(Exception):
            self.common()

    @patch.object(DIDClient, "add_dataset", side_effect=RucioException("failed"))
    @patch.object(DIDClient, "add_files_to_dataset", side_effect=DataIdentifierNotFound("failed"))
    def testException8TestCase(self, MC1, MC2):
        with self.assertRaises(Exception):
            self.common()

    @patch.object(DIDClient, "add_files_to_dataset", side_effect=RucioException("failed"))
    def testException9Case(self, MC1):
        rucio_rse = "DRR1"
        scope = "test"
        dtn_url = "root://xrd1:1094//rucio"

        ri = RucioInterface(self.butler, rucio_rse, scope, self.rse_root, dtn_url, DataType.DATA_PRODUCT)
        with self.assertRaises(Exception):
            ri._add_file_to_dataset_with_retries(None, None)

    def tearDown(self):
        patch.stopall()
        shutil.rmtree(self.butler_repo, ignore_errors=True)
        shutil.rmtree(self.rse_root, ignore_errors=True)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
