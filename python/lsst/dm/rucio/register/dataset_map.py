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

import yaml


class DatasetMap:
    """Mapping of dataset type to dataset templates

    Parameters
    ----------
    mapping_yaml: `str`
        name of YAML file containing mapping
    """

    def __init__(self, mapping_yaml):
        with open(mapping_yaml) as f:
            self.config = yaml.safe_load(f)

    def get_dataset_template(self, typename) -> str:
        """Get the dataset_template associated with a dataset type

        Parameters
        ----------
        typename: `str`
            Butler typename
        """
        template = self.config[typename]["dataset_template"]
        return template
