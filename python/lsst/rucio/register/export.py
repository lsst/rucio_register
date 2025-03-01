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


from typing import Any

import click

from lsst.daf.butler import Butler, CollectionType, FileDataset
from lsst.daf.butler.cli.opt import query_datasets_options


@click.command(short_help="Export registry information about datasets.")
@click.option("--root", help="URI root for existing direct ingests to be stripped.")
@click.option("--filename", default="export.yaml", help="Output filename (default=export.yaml).")
@query_datasets_options(repo=True, useArguments=True, use_order_by=False, showUri=False)
def main(repo: str, glob: Any, filename: str, root: str | None = None, **kwargs: Any) -> None:
    """Export the registry information about datasets selected by collection,
    dataset type, and where clause to an export.yaml file.
    """
    if "collections" not in kwargs:
        raise RuntimeError("Collection(s) option is required.")

    butler = Butler(repo)
    ds_types = set([ds.name for ds in butler.registry.queryDatasetTypes(glob)])

    if root is not None:
        if not root.endswith("/"):
            root += "/"

        def rewrite_file_dataset(file_dataset: FileDataset) -> FileDataset:
            file_dataset.path = file_dataset.path.removeprefix(root)
            return file_dataset

        rewrite = rewrite_file_dataset
        print(f"Stripping root prefix: {root}")
    else:
        rewrite = None

    print(f"Output to: {filename}")
    with butler.export(filename=filename, format="yaml", transfer=None) as export:
        actual_ds_types = set()

        # We only save calibration and run collections, not complete chains.
        # This is expected to be more useful for DRP.
        for collection_info in butler.collections.query_info(
            kwargs["collections"],
            flatten_chains=True,
            include_summary=True,
            summary_datasets=ds_types,
        ):
            present_ds_types = ds_types & collection_info.dataset_types
            if present_ds_types:
                if collection_info.type == CollectionType.CALIBRATION:
                    print(f"Saving collection associations: {collection_info.name}")
                    export.saveCollection(collection_info.name)
                actual_ds_types |= present_ds_types

        for ds_type in actual_ds_types:
            print(f"Saving dataset type: {ds_type}")
            export.saveDatasets(
                butler.query_datasets(ds_type, with_dimension_records=True, **kwargs),
                rewrite=rewrite,
            )
