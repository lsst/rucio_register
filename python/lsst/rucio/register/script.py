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
from typing import Any

import click

from lsst.daf.butler import Butler
from lsst.daf.butler.cli.opt import options_file_option, query_datasets_options
from lsst.daf.butler.script.queryDatasets import QueryDatasets
from lsst.rucio.register.data_type import DataType
from lsst.rucio.register.rucio_interface import RucioInterface
from lsst.rucio.register.rucio_register_config import RucioRegisterConfig

logger = logging.getLogger(__name__)
_FORMAT = (
    "%(levelname) -10s %(asctime)s.%(msecs)03dZ %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s"
)

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


def _getRucioInterface(repo, rucio_register_config, rubin_butler_type):
    # default to using RUCIO_REGISTER_CONFIG env variable
    # if that's not set, try to use the command line
    # if neither are set, then raise an Exception
    config_file = os.environ.get(RUCIO_REGISTER_CONFIG, rucio_register_config)
    if config_file is None:
        raise RuntimeError(f"{RUCIO_REGISTER_CONFIG} {_MSG}")

    config = RucioRegisterConfig(config_file)

    rucio_rse = config.rucio_rse
    scope = config.scope
    rse_root = config.rse_root
    dtn_url = config.dtn_url

    butler = Butler(repo)

    # create RucioInterface object used to register replicas into datasets
    ri = RucioInterface(
        butler=butler,
        rucio_rse=rucio_rse,
        scope=scope,
        rse_root=rse_root,
        dtn_url=dtn_url,
        rubin_butler_type=rubin_butler_type,
    )
    return ri, butler


def _register(ri, dataset_refs, chunk_size, rucio_dataset):
    # register dataset_refs with Rucio into the rucio dataset, in chunks
    for refs in chunks(dataset_refs, chunk_size):
        logger.debug(f"x: {chunk_size=}, {rucio_dataset=}, {refs=}")
        cnt = ri.register_as_replicas(rucio_dataset, refs)
        logger.debug(f"{cnt} butler datasets registered")


def _set_log_level(debug):
    if debug is None:
        log_level = logging.WARNING
    else:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level, format=(_FORMAT), datefmt="%Y-%m-%d %H:%M:%S")


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def main():
    pass


@click.option("-r", "--repo", required=True, type=str, help="butler repository")
@click.option("-c", "--collections", required=True, type=str, help="collections for lookup")
@click.option("-t", "--dataset-type", required=True, type=str, help="dataset type for lookup")
@click.option("-d", "--rucio-dataset", required=True, type=str, help="rucio dataset to register files to")
@click.option(
    "-C", "--rucio-register-config", required=False, type=str, help="configuration file used for registration"
)
@click.option("-D", "--debug", required=False, is_flag=True, help="set loglevel to DEBUG")
@click.option(
    "-s",
    "--chunk-size",
    required=False,
    type=int,
    default=30,
    help="number of replica requests to make at once",
)
@main.command()
def data_products(repo, collections, dataset_type, rucio_dataset, rucio_register_config, debug, chunk_size):
    _set_log_level(debug)

    ri, butler = _getRucioInterface(repo, rucio_register_config, DataType.DATA_PRODUCT)

    # query the butler for the datasets specified on the commmand line
    dataset_refs = butler.registry.queryDatasets(dataset_type, collections=collections)

    _register(ri, dataset_refs, chunk_size, rucio_dataset)


def _get_and_delete(kwargs, key):
    x = kwargs.get(key, None)
    if x is None:
        return x
    del kwargs[key]
    return x


@main.command()
@click.option("-r", "--repo", required=True, type=str, help="butler repository")
@click.option("-d", "--rucio-dataset", required=True, type=str, help="rucio dataset to register files to")
@click.option(
    "-C", "--rucio-register-config", required=False, type=str, help="configuration file used for registration"
)
@click.option(
    "-s",
    "--chunk-size",
    required=False,
    type=int,
    default=30,
    help="number of replica requests to make at once",
)
@click.option("-D", "--debug", required=False, is_flag=True, help="set loglevel to DEBUG")
@options_file_option()
@query_datasets_options(repo=False, showUri=True)
def raws(**kwargs: Any) -> None:
    # get and delete from kwargs; QueryDatasets doesn't like extra args
    debug = _get_and_delete(kwargs, "debug")
    _set_log_level(debug)

    rucio_register_config = _get_and_delete(kwargs, "rucio_register_config")
    rucio_dataset = _get_and_delete(kwargs, "rucio_dataset")
    chunk_size = _get_and_delete(kwargs, "chunk_size")

    repo = kwargs["repo"]

    ri, butler = _getRucioInterface(repo, rucio_register_config, DataType.RAW_FILE)

    dataset_refs = QueryDatasets(**kwargs).getDatasets()

    _register(ri, dataset_refs, chunk_size, rucio_dataset)
