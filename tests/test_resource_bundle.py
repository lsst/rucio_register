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


import unittest

import lsst.utils.tests
from lsst.dm.rucio.register.resource_bundle import ResourceBundle


class ResourceBundleTestCase(unittest.TestCase):

    def testResourceBundle(self):
        rb = ResourceBundle("12", {"1": 1, "2": 2, "3": 3})

        self.assertEqual(rb.dataset_id, "12")
        self.assertEqual(rb.did["1"], 1)
        self.assertEqual(rb.did["2"], 2)
        self.assertEqual(rb.did["3"], 3)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
