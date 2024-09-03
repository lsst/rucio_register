# This file is part of rucio_register
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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


import itertools
import logging
import os
import sys

from lsst.daf.butler import Butler
from lsst.rucio.register.app_parser import AppParser
from lsst.rucio.register.rucio_interface import RucioInterface
from lsst.rucio.register.rucio_register_config import RucioRegisterConfig

logger = logging.getLogger(__name__)

RUCIO_REGISTER_CONFIG = "RUCIO_REGISTER_CONFIG"
_MSG = "environment variable not set, and no configuration was specified on the command line"


def chunks(refs, chunk_size):
    it = iter(refs)
    while True:
        chunk = itertools.islice(it, chunk_size)
        try:
            start = next(chunk)
        except StopIteration:
            return
        yield itertools.chain((start,), chunk)


def main():
    """Command line interface for rucio-register"""
    ap = AppParser(sys.argv)

    # default to using RUCIO_REGISTER_CONFIG env variable
    # if that's not set, try to use the command line
    # if neither are set, then raise an Exception
    config_file = os.environ.get(RUCIO_REGISTER_CONFIG, ap.register_config)
    if config_file is None:
        raise RuntimeError(f"{RUCIO_REGISTER_CONFIG} {_MSG}")

    config = RucioRegisterConfig(config_file)

    rucio_rse = config.rucio_rse
    scope = config.scope
    rse_root = config.rse_root
    dtn_url = config.dtn_url

    butler = Butler(ap.butler_repo)

    # create RucioInterface object used to register replicas into datasets
    ri = RucioInterface(
        butler=butler,
        rucio_rse=rucio_rse,
        scope=scope,
        rse_root=rse_root,
        dtn_url=dtn_url,
    )

    # query the butler for the datasets specified on the commmand line
    dataset_refs = butler.registry.queryDatasets(ap.dataset_type, collections=ap.collections)

    # register dataset_refs with Rucio into the rucio dataset, in chunks
    for refs in chunks(dataset_refs, ap.chunks):
        cnt = ri.register_as_replicas(ap.rucio_dataset, refs)
        logger.debug(f"{cnt} butler datasets registered")
