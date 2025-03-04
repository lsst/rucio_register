from typing import Any

import click

from lsst.daf.butler import Butler, CollectionType, FileDataset
from lsst.daf.butler.cli.opt import query_datasets_options


def strip_root(file_dataset: FileDataset, root: str) -> FileDataset:
    file_dataset.path = file_dataset.path.removeprefix(root)
    return file_dataset


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
            return strip_root(file_dataset, root)

        rewrite = rewrite_file_dataset
        print(f"Stripping root prefix: {root}")
    else:
        rewrite = None

    print(f"Output to: {filename}")
    with butler.export(filename=filename, format="yaml", transfer=None) as export:
        actual_ds_types = set()

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
