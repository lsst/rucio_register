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


import unittest

import lsst.utils.tests
from lsst.rucio.register.data_type import DataType
from lsst.rucio.register.resource_bundle import ResourceBundle
from lsst.rucio.register.rubin_meta import RubinMeta
from lsst.rucio.register.rucio_did import RucioDID


class ResourceBundleTestCase(unittest.TestCase):
    def testResourceBundle(self):
        meta = RubinMeta(rubin_butler=DataType.DATA_PRODUCT, rubin_sidecar="mysidecar")
        did = RucioDID(
            pfn="string1", bytes=451, adler32="32", md5="0x5", name="myname", scope="mouthwash", meta=meta
        )
        rb = ResourceBundle(dataset_id="12", did=did)

        self.assertEqual(rb.dataset_id, "12")
        rdid = rb.get_did()
        self.assertEqual(rdid["pfn"], "string1")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
