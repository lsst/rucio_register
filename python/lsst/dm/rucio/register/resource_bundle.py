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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging

__all__ = ["ResourceBundle"]

logger = logging.getLogger(__name__)


class ResourceBundle:
    def __init__(self, dataset_id: str, did: dict):
        """Create a resource bundle of a dataset_id and its metadata

        Parameters
        ----------
        dataset_id: `str`
            dataset_id
        did: `dict`
            dataset dictionary
        """
        self.dataset_id = dataset_id
        self.did = did

    def get_dataset_id(self) -> str:
        return self.dataset_id

    def get_did(self) -> str:
        return self.did
