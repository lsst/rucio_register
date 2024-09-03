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
import unittest

import lsst.utils.tests
from lsst.rucio.register.dataset_map import DatasetMap


class DatasetMapTestCase(unittest.TestCase):

    def testDatasetMap(self):
        test_dir = os.path.abspath(os.path.dirname(__file__))
        map_file = os.path.join(test_dir, "data", "map.yaml")
        td = DatasetMap.from_yaml(map_file)

        s = td.map["visitSummary"]
        self.assertEqual(s, "{visit}/one/two")

        s = td.map["isolated_star_cat"]
        self.assertEqual(s, "three/four/{tract}")

        s = td.map["isolated_star_sources"]
        self.assertEqual(s, "five/six/{tract}")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
