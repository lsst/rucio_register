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

import argparse
import logging

_FORMAT = (
    "%(levelname) -10s %(asctime)s.%(msecs)03dZ %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s"
)


class AppParser:
    """Application command line argument parser

    Parameters
    ----------
    argv: `list`
        list of program name and arguments
    """

    def __init__(self, argv):
        parser = argparse.ArgumentParser(prog=argv[0])
        parser.add_argument(
            "-r",
            "--repo",
            action="store",
            default=None,
            dest="repo",
            help="Butler repository",
            type=str,
            required=True,
        )
        parser.add_argument(
            "-c",
            "--collections",
            action="store",
            default=None,
            dest="collections",
            help="collections for lookup",
            type=str,
            required=True,
        )

        parser.add_argument(
            "-t",
            "--dataset-type",
            action="store",
            default=None,
            dest="dataset_type",
            help="dataset type for lookup",
            type=str,
            required=True,
        )

        parser.add_argument(
            "-d",
            "--rucio-dataset",
            action="store",
            default=None,
            dest="rucio_dataset",
            help="rucio dataset to register files to",
            type=str,
            required=True,
        )
        parser.add_argument(
            "-C",
            "--rucio-register-config",
            action="store",
            default=None,
            dest="register_config",
            help="configuration file used for registration",
            type=str,
            required=False,
        )
        parser.add_argument(
            "-s",
            "--chunk-size",
            help="number of replica requests to make at once",
            action="store",
            dest="chunks",
            type=int,
            required=False,
            default=30,
        )
        # the following arguments are for logging;
        # defaults to WARNING
        # -v sets to INFO
        # -D sets to DEBUG

        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-v",
            "--verbose",
            help="set loglevel to INFO",
            action="store_const",
            dest="loglevel",
            const=logging.INFO,
            default=None,
        )
        group.add_argument(
            "-D",
            "--debug",
            help="set loglevel to DEBUG",
            action="store_const",
            dest="loglevel",
            const=logging.DEBUG,
            default=None,
        )

        args = parser.parse_args(argv[1:])

        if args.loglevel is None:
            loglevel = logging.WARNING
        else:
            loglevel = args.loglevel

        logging.basicConfig(level=loglevel, format=(_FORMAT), datefmt="%Y-%m-%d %H:%M:%S")

        self.butler_repo = args.repo
        self.collections = args.collections
        self.dataset_type = args.dataset_type
        self.rucio_dataset = args.rucio_dataset
        self.register_config = args.register_config
        self.loglevel = args.loglevel
        self.chunks = args.chunks
