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


import json
import logging
import os
import tempfile
import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

import lsst.utils.tests
from lsst.daf.butler import _exceptions
from lsst.rucio.register import script


class LimitDatasetTypesTestCase(unittest.TestCase):
    def test_limit_dataset_types(self):
        dataset_types = ["a", "b", "c", "d"]

        self.assertEqual(script._limit_dataset_types(dataset_types, None), dataset_types)
        self.assertEqual(script._limit_dataset_types(dataset_types, 2), ["a", "b"])
        self.assertEqual(script._limit_dataset_types(dataset_types, 0), [])


class SetLogLevelTestCase(unittest.TestCase):
    def test_set_log_level(self):
        # String input
        script._set_log_level("DEBUG")
        log = logging.getLogger()
        self.assertEqual(log.level, logging.DEBUG)
        self.assertTrue(len(log.handlers) > 0)

        # Dict input from daf_butler cli log_level_option
        script._set_log_level({None: "INFO"})
        self.assertEqual(log.level, logging.INFO)


class ChunksTestCase(unittest.TestCase):
    def test_chunks_materialized_list(self):
        dataset_refs = list(range(10))
        ref_chunks = list(script.chunks(dataset_refs, 3))
        self.assertEqual(ref_chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

    def test_chunks_exact_multiple(self):
        dataset_refs = list(range(9))
        ref_chunks = list(script.chunks(dataset_refs, 3))
        self.assertEqual(ref_chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8]])

    def test_chunks_empty(self):
        ref_chunks = list(script.chunks([], 3))
        self.assertEqual(ref_chunks, [])

    def test_chunks_chunk_size_larger_than_input(self):
        dataset_refs = [1, 2, 3]
        ref_chunks = list(script.chunks(dataset_refs, 10))
        self.assertEqual(ref_chunks, [[1, 2, 3]])


class DummyExecutor:
    def __init__(self, max_workers=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def submit(self, fn, *args, **kwargs):
        fut = Future()
        try:
            res = fn(*args, **kwargs)
            fut.set_result(res)
        except Exception as e:
            fut.set_exception(e)
        return fut


class AutoRegisterTestCase(lsst.utils.tests.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.out_dir = os.path.join(self.tmp_dir.name, "uuids")
        self.repo = "/tmp/test_repo"

    def tearDown(self):
        self.tmp_dir.cleanup()

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_dry_run_no_raw(self, mock_butler_class):
        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_butler.collections.query.return_value = ["HSC/runs/RC2/w_2023_32"]

        mock_info = MagicMock()
        mock_info.dataset_types = ["visitSummary", "deepCoadd"]
        mock_butler.collections.get_info.return_value = mock_info

        ref1 = MagicMock()
        ref1.id = "11111111-1111-1111-1111-111111111111"
        ref2 = MagicMock()
        ref2.id = "22222222-2222-2222-2222-222222222222"
        mock_butler.query_all_datasets.return_value = [ref1, ref2]

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)

        sanitized_repo = self.repo.strip("/").replace("/", "_")
        expected_dir = os.path.join(self.out_dir, sanitized_repo, "auto_register")
        self.assertTrue(os.path.exists(expected_dir))

        files = os.listdir(expected_dir)
        self.assertGreater(len(files), 0)

        with open(os.path.join(expected_dir, files[0])) as f:
            content = f.read()

        self.assertIn("11111111-1111-1111-1111-111111111111", content)
        self.assertIn("22222222-2222-2222-2222-222222222222", content)

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script._get_rucio_interface")
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_dry_run_with_raw(self, mock_butler_class, mock_get_ri):
        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_butler.collections.query.return_value = ["HSC/runs/RC2/w_2023_32"]

        mock_info = MagicMock()
        mock_info.dataset_types = ["raw", "visitSummary"]
        mock_butler.collections.get_info.return_value = mock_info

        mock_ri = MagicMock()
        mock_inner_butler = MagicMock()
        mock_get_ri.return_value = (mock_ri, mock_inner_butler)

        ref1 = MagicMock()
        ref1.id = "33333333-3333-3333-3333-333333333333"
        mock_butler.query_datasets.return_value = [ref1]

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        sanitized_repo = self.repo.strip("/").replace("/", "_")
        expected_dir = os.path.join(self.out_dir, sanitized_repo, "auto_register")
        self.assertTrue(os.path.exists(expected_dir))
        files = os.listdir(expected_dir)
        self.assertGreater(len(files), 0)

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script._register", return_value=2)
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_registration_mode(self, mock_butler_class, mock_register):
        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_butler.collections.query.return_value = ["HSC/runs/RC2/w_2023_32"]

        mock_info = MagicMock()
        mock_info.dataset_types = ["visitSummary"]
        mock_butler.collections.get_info.return_value = mock_info

        ref1 = MagicMock()
        ref1.id = "44444444-4444-4444-4444-444444444444"
        mock_butler.query_all_datasets.return_value = [ref1]

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--out-dir",
                self.out_dir,
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertTrue(mock_register.called)

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_empty_collections_and_exceptions(self, mock_butler_class):
        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler

        # Empty collections
        mock_butler.collections.query.return_value = []
        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)

        # None info or empty dataset_types
        mock_butler.collections.query.return_value = ["HSC/runs/empty"]
        mock_butler.collections.get_info.return_value = None
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)

        # mock_info with dataset_types = None
        mock_info = MagicMock()
        mock_info.dataset_types = None
        mock_butler.collections.get_info.return_value = mock_info
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)

        # EmptyQueryResultError
        mock_info = MagicMock()
        mock_info.dataset_types = ["visitSummary"]
        mock_butler.collections.get_info.return_value = mock_info
        mock_butler.query_all_datasets.side_effect = _exceptions.EmptyQueryResultError(
            reasons=["No datasets found"]
        )
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)

        # Generic Exception
        mock_butler.query_all_datasets.side_effect = Exception("Query error")
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )
        self.assertEqual(result.exit_code, 0, msg=result.output)

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script._get_rucio_interface")
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_max_dataset_types(self, mock_butler_class, mock_get_ri):
        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_butler.collections.query.return_value = ["HSC/runs/RC2/w_2023_32"]

        mock_info = MagicMock()
        mock_info.dataset_types = ["typeA", "typeB", "typeC"]
        mock_butler.collections.get_info.return_value = mock_info

        mock_ri = MagicMock()
        mock_inner_butler = MagicMock()
        mock_get_ri.return_value = (mock_ri, mock_inner_butler)
        mock_butler.query_datasets.return_value = []

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--max-dataset-types",
                "1",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        called_dataset_types = [
            call.kwargs.get("dataset_type") for call in mock_butler.query_datasets.call_args_list
        ]
        self.assertIn("typeA", called_dataset_types)
        self.assertNotIn("typeB", called_dataset_types)
        self.assertNotIn("typeC", called_dataset_types)

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_direct_call(self, mock_butler_class):
        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_butler.collections.query.return_value = []

        script.auto_register.callback(
            repo=self.repo,
            root_chain="HSC/runs/*",
            start_date="2023-01-01",
            cutoff_date="2023-04-01",
            df_name="USDF",
            max_did_per_dataset=50000,
            dry_run=True,
            out_dir=self.out_dir,
            log_level={},
        )
        mock_butler.collections.query.assert_called_once()

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script._get_rucio_interface")
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_with_transfer_list_file(self, mock_butler_class, mock_get_ri):
        yaml_content = """
        stage1:
          step1a:
            - typeA
            - typeC
        """
        temp_yaml_path = os.path.join(self.tmp_dir.name, "test_transfer_list.yaml")
        with open(temp_yaml_path, "w") as f:
            f.write(yaml_content)

        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_butler.collections.query.return_value = ["HSC/runs/RC2/w_2023_32"]

        mock_info = MagicMock()
        mock_info.dataset_types = ["raw", "typeA", "typeB", "typeC"]
        mock_butler.collections.get_info.return_value = mock_info

        mock_ri = MagicMock()
        mock_inner_butler = MagicMock()
        mock_get_ri.return_value = (mock_ri, mock_inner_butler)
        mock_butler.query_datasets.return_value = []

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--transfer-list",
                temp_yaml_path,
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        called_dataset_types = [
            call.kwargs.get("dataset_type") for call in mock_butler.query_datasets.call_args_list
        ]
        self.assertIn("typeA", called_dataset_types)
        self.assertIn("typeC", called_dataset_types)
        self.assertNotIn("typeB", called_dataset_types)
        self.assertNotIn("raw", called_dataset_types)

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script.urllib.request.urlopen")
    @patch("lsst.rucio.register.script._get_rucio_interface")
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_with_transfer_list_url(self, mock_butler_class, mock_get_ri, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = b"""
        stage1:
          step1b:
            - typeB
        """
        mock_urlopen.return_value.__enter__.return_value = mock_response

        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_butler.collections.query.return_value = ["HSC/runs/RC2/w_2023_32"]

        mock_info = MagicMock()
        mock_info.dataset_types = ["typeA", "typeB", "typeC"]
        mock_butler.collections.get_info.return_value = mock_info

        mock_ri = MagicMock()
        mock_inner_butler = MagicMock()
        mock_get_ri.return_value = (mock_ri, mock_inner_butler)

        refA = MagicMock()
        refA.id = "11111111-1111-1111-1111-111111111111"
        refA.dataset_type.name = "typeA"
        refB = MagicMock()
        refB.id = "22222222-2222-2222-2222-222222222222"
        refB.dataset_type.name = "typeB"
        refC = MagicMock()
        refC.id = "33333333-3333-3333-3333-333333333333"
        refC.dataset_type.name = "typeC"

        mock_butler.query_all_datasets.return_value = [refA, refB, refC]

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--transfer-list",
                "https://raw.githubusercontent.com/test/cm_transfer_list.yaml",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        sanitized_repo = self.repo.strip("/").replace("/", "_")
        expected_dir = os.path.join(self.out_dir, sanitized_repo, "auto_register")
        self.assertTrue(os.path.exists(expected_dir))
        files = os.listdir(expected_dir)
        self.assertGreater(len(files), 0)

        with open(os.path.join(expected_dir, files[0])) as f:
            content = f.read()

        self.assertIn("22222222-2222-2222-2222-222222222222", content)
        self.assertNotIn("11111111-1111-1111-1111-111111111111", content)
        self.assertNotIn("33333333-3333-3333-3333-333333333333", content)

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script._get_rucio_interface")
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_with_dataset_name_prefix(self, mock_butler_class, mock_get_ri):
        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_butler.collections.query.return_value = ["HSC/runs/RC2/w_2023_32"]

        mock_info = MagicMock()
        mock_info.dataset_types = ["typeA"]
        mock_butler.collections.get_info.return_value = mock_info

        mock_ri = MagicMock()
        mock_inner_butler = MagicMock()
        mock_get_ri.return_value = (mock_ri, mock_inner_butler)

        refA = MagicMock()
        refA.id = "11111111-1111-1111-1111-111111111111"
        refA.dataset_type.name = "typeA"

        mock_butler.query_all_datasets.return_value = [refA]

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--dataset-name-prefix",
                "Dataset/HSC/runs/RC2/w_2026_23/DM-55175/LANCS/outputs",
                "--dry-run",
                "--out-dir",
                self.out_dir,
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        sanitized_repo = self.repo.strip("/").replace("/", "_")
        expected_dir = os.path.join(self.out_dir, sanitized_repo, "auto_register")
        self.assertTrue(os.path.exists(expected_dir))
        files = os.listdir(expected_dir)
        self.assertTrue(
            any("Dataset-HSC-runs-RC2-w_2026_23-DM-55175-LANCS-outputs-typeA-USDF" in f for f in files)
        )

    @patch("lsst.rucio.register.script.ProcessPoolExecutor", DummyExecutor)
    @patch("lsst.rucio.register.script._process_auto_register_batch")
    @patch("lsst.rucio.register.script.Butler")
    def test_auto_register_with_clear_is_new(self, mock_butler_class, mock_process_batch):
        mock_butler = MagicMock()
        mock_butler_class.return_value = mock_butler
        mock_process_batch.return_value = {"registered": 1, "failed": 0}

        mock_butler.collections.query.return_value = ["HSC/runs/RC2/w_2023_32"]
        mock_info = MagicMock()
        mock_info.dataset_types = ["typeA"]
        mock_butler.collections.get_info.return_value = mock_info

        refA = MagicMock()
        refA.id = "11111111-1111-1111-1111-111111111111"
        refA.dataset_type.name = "typeA"
        mock_butler.query_all_datasets.return_value = [refA]

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "auto-register",
                "--repo",
                self.repo,
                "--root-chain",
                "HSC/runs/*",
                "--start-date",
                "2023-01-01",
                "--cutoff-date",
                "2023-04-01",
                "--clear-is-new",
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertTrue(mock_process_batch.called)
        _, kwargs = mock_process_batch.call_args
        self.assertTrue(kwargs.get("clear_is_new"))

    @patch("lsst.rucio.register.script._get_rucio_interface")
    def test_dataset_list_with_json_uuidlist(self, mock_get_ri):
        mock_ri = MagicMock()
        mock_butler = MagicMock()
        mock_get_ri.return_value = (mock_ri, mock_butler)

        json_file = os.path.join(self.tmp_dir.name, "auto-register-failures.json")
        with open(json_file, "w") as f:
            json.dump(["019ee079-b7e4-700d-a0bf-03412ae4fe0a"], f)

        mock_ref = MagicMock()
        mock_ref.id = "019ee079-b7e4-700d-a0bf-03412ae4fe0a"
        mock_butler.get_many_datasets.return_value = [mock_ref]

        runner = CliRunner()
        result = runner.invoke(
            script.main,
            [
                "dataset-list",
                "--repo",
                self.repo,
                "--rucio-dataset",
                "Dataset/Test/Failed",
                "--uuidlist",
                json_file,
            ],
        )

        self.assertEqual(result.exit_code, 0, msg=result.output)
        mock_butler.get_many_datasets.assert_called_once_with(["019ee079-b7e4-700d-a0bf-03412ae4fe0a"])


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
