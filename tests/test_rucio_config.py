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
from lsst.rucio.register.rucio_register_config import RucioRegisterConfig


class RucioRegisterConfigTestCase(unittest.TestCase):

    def testConfig1(self):
        test_dir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(test_dir, "data", "config1.yaml")
        rrc = RucioRegisterConfig(config_file)
        self.assertEqual(rrc.rucio_rse, "RSE1")
        self.assertEqual(rrc.scope, "testscope")
        self.assertEqual(rrc.rse_root, "/rse/root")
        self.assertEqual(rrc.dtn_url, "root://rse1:1094//rucio")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
