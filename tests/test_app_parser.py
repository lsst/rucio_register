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


import logging
import unittest

import lsst.utils.tests
from lsst.rucio.register.app_parser import AppParser


class AppParserTestCase(unittest.TestCase):

    def testAppParser(self):
        argv = [
            "unittest",
            "-r",
            "repo",
            "-c",
            "collections",
            "-t",
            "type",
            "-d",
            "dataset",
            "-C",
            "config",
            "-v",
        ]
        ap = AppParser(argv)

        self.assertEqual(ap.butler_repo, "repo")
        self.assertEqual(ap.collections, "collections")
        self.assertEqual(ap.dataset_type, "type")
        self.assertEqual(ap.rucio_dataset, "dataset")
        self.assertEqual(ap.register_config, "config")
        self.assertEqual(ap.loglevel, logging.INFO)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
