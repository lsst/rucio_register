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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import json
import os
import shutil
import tempfile
from unittest.mock import MagicMock

import lsst.utils.tests
from lsst.daf.butler import Butler, DatasetRef, FileDataset
from lsst.daf.butler.registry import DatasetTypeError, MissingCollectionError
from lsst.dm.rucio.register.rucio_interface import RucioInterface
from lsst.pipe.base import Instrument
from lsst.resources import ResourcePath


class InterfaceTestCase(lsst.utils.tests.TestCase):

    def setUp(self):
        self.butler_repo = tempfile.mkdtemp()

        instr = Instrument.from_string("lsst.obs.subaru.HyperSuprimeCam")

        Butler.makeRepo(self.butler_repo)
        self.butler = Butler(self.butler_repo, writeable=True)
        instr.register(self.butler.registry)

        test_dir = os.path.abspath(os.path.dirname(__file__))

        dim_exposure = "dim_exposure.yaml"
        dim_visit = "dim_visit.yaml"

        self.butler.import_(filename=os.path.join(test_dir, "data", dim_exposure))
        self.butler.import_(filename=os.path.join(test_dir, "data", dim_visit))

        data_name = "visitSummary_HSC_y_HSC-Y_318_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.fits"
        json_name = "visitSummary_HSC_y_HSC-Y_318_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.json"

        self.data_file = os.path.join(test_dir, "data", data_name)
        self.json_file = os.path.join(test_dir, "data", json_name)

        fds = self.create_file_dataset(self.data_file, self.json_file)
        self.ingest([fds])

    def testInterfaceTestCase(self):
        rucio_rse = "DRR1"
        scope = "test"
        rse_root = tempfile.mkdtemp()
        dtn_url = "root://xrd1:1094//rucio"

        ri = RucioInterface(self.butler, rucio_rse, scope, rse_root, dtn_url)
        ri._add_replicas = MagicMock(name="_add_replicas")
        ri.register_to_dataset = MagicMock(name="register_to_dataset")

        self.butler.registry.refresh()

        dataset_refs = self.butler.registry.queryDatasets(
            "visitSummary",
            collections="HSC/runs/RC2/w_2023_32/DM-40356/20230814T170253Z",
        )
        cnt = ri.register_as_replicas("mydataset", dataset_refs)
        self.assertEqual(cnt, 1)

        dataset_refs = self.butler.registry.queryDatasets(
            "visitSummary",
            collections="HSC/runs/RC2/w_2023_32/DM-40356/20230814T170253Z",
        )
        cnt = 0
        for ref in dataset_refs:
            self.assertEqual(cnt, 0, "There should have been only one dataset ref retrieved")

            rb = ri._make_bundle("mydataset", ref)
            self.assertEqual(rb.dataset_id, "mydataset")

            did = rb.did.model_dump()
            self.assertEqual(did["pfn"], f"{dtn_url}{self.data_file}")
            self.assertEqual(did["bytes"], 1365120)
            self.assertEqual(did["adler32"], "480be4de")
            self.assertEqual(did["md5"], "a7ee5c19f5717bcf8d772de202864244")
            self.assertEqual(did["name"], f"{self.data_file}")
            self.assertEqual(did["scope"], "test")

            meta = did["meta"]
            self.assertEqual(meta["rubin_butler"], 1)

            with open(self.json_file, "r") as f:
                metadata = json.loads(f.readline())
            sidecar = json.loads(meta["rubin_sidecar"])
            self.assertDictEqual(metadata, sidecar)
            cnt = cnt + 1

    def tearDown(self):
        shutil.rmtree(self.butler_repo, ignore_errors=True)

    def create_file_dataset(self, datafile: str, jsonfile: str):
        with open(jsonfile, "r") as f:
            metadata = f.readline()
        ref = DatasetRef.from_json(metadata, registry=self.butler.registry)
        fds = FileDataset(ResourcePath(f"file://{datafile}"), ref)
        return fds

    def ingest(self, datasets: list):
        """Ingest a list of Datasets

        Parameters
        ----------
        filedataset : `FileDataset`
            A FileDataset
        """
        completed = False
        while not completed:
            try:
                self.butler.ingest(*datasets, transfer="direct")
                print("ingested")
                completed = True
            except DatasetTypeError:
                print("DatasetTypeError")
                dst_set = set()
                for dataset in datasets:
                    for dst in {ref.datasetType for ref in dataset.refs}:
                        dst_set.add(dst)
                for dst in dst_set:
                    self.butler.registry.registerDatasetType(dst)
            except MissingCollectionError:
                print("MissingCollectionError")
                run_set = set()
                for dataset in datasets:
                    for run in {ref.run for ref in dataset.refs}:
                        run_set.add(run)
                for run in run_set:
                    self.butler.registry.registerRun(run)
            except Exception as e:
                print(f"Exception {e=}")
                completed = True


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
