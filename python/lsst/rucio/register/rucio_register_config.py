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
import yaml


class RucioRegisterConfig:
    """Describes a Rucio configuration

    Parameters
    ----------
    config_file: `str`
       path to configuration file
    """

    def __init__(self, config_file: str):
        with open(config_file) as f:
            config = yaml.safe_load(f)

        self.rucio_rse = config["rucio_rse"]
        self.scope = config["scope"]
        self.rse_root = config["rse_root"]
        self.dtn_url = config["dtn_url"]
