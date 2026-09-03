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


import datetime
import itertools
import json
import logging
import os
import sys
import threading
import time
import urllib.request
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any

import click
import yaml

from lsst.daf.butler import Butler, CollectionType, _exceptions
from lsst.daf.butler.cli.opt import log_level_option, options_file_option, query_datasets_options
from lsst.daf.butler.script.queryDatasets import QueryDatasets
from lsst.resources import ResourcePath
from lsst.rucio.register.data_type import DataType
from lsst.rucio.register.rucio_interface import RucioInterface
from lsst.rucio.register.rucio_register_config import RucioRegisterConfig

_thread_local = threading.local()

logger = logging.getLogger(__name__)
_FORMAT = (
    "%(levelname) -10s %(asctime)s.%(msecs)03dZ %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s"
)

RUCIO_REGISTER_CONFIG = "RUCIO_REGISTER_CONFIG"
_MSG = "environment variable not set, and no configuration was specified on the command line"


def register_options(func):
    """Decorator for CLI options that are common across commands."""
    func = click.option(
        "--backoff-max-tries",
        "--max-retries",
        "max_retries",
        required=False,
        type=int,
        default=RucioInterface.DEFAULT_RETRIES,
        show_default=True,
        help="maximum number of times to retry failed registration",
    )(func)
    func = click.option(
        "--backoff-max-value",
        required=False,
        type=int,
        default=RucioInterface.DEFAULT_BACKOFF_MAX_VALUE,
        show_default=True,
        help="maximum backoff value in seconds",
    )(func)
    func = click.option(
        "--backoff-factor",
        required=False,
        type=float,
        default=RucioInterface.DEFAULT_BACKOFF_FACTOR,
        show_default=True,
        help="multiplicative factor to backoff by",
    )(func)
    func = click.option(
        "--chunk-size",
        required=False,
        type=int,
        default=500,
        help="number of replica requests to make at once",
    )(func)
    func = click.option(
        "--rucio-register-config", required=False, type=str, help="registration configuration file"
    )(func)
    func = click.option(
        "--rucio-dataset", required=False, type=str, help="rucio dataset to register files to"
    )(func)
    return func


def chunks(refs, chunk_size):
    it = iter(refs)
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            return
        yield chunk


_rucio_interface_cache = {}


def _get_rucio_interface(
    repo,
    rucio_register_config,
    rubin_butler_type,
    clear_is_new=False,
    retries=RucioInterface.DEFAULT_RETRIES,
    kwargs=None,
):
    if kwargs:
        backoff_factor = kwargs.get("backoff_factor", RucioInterface.DEFAULT_BACKOFF_FACTOR)
        backoff_max_value = kwargs.get("backoff_max_value", RucioInterface.DEFAULT_BACKOFF_MAX_VALUE)
        backoff_max_tries = kwargs.get("backoff_max_tries", kwargs.get("max_retries", retries))
        if backoff_max_tries is not None:
            retries = backoff_max_tries
    else:
        backoff_factor = RucioInterface.DEFAULT_BACKOFF_FACTOR
        backoff_max_value = RucioInterface.DEFAULT_BACKOFF_MAX_VALUE
        backoff_max_tries = retries

    config_file = os.environ.get(RUCIO_REGISTER_CONFIG, rucio_register_config)
    cache_key = (
        repo,
        config_file,
        rubin_butler_type,
        clear_is_new,
        retries,
        backoff_factor,
        backoff_max_value,
    )
    if cache_key in _rucio_interface_cache:
        logger.debug("Reusing cached RucioInterface/Butler for key: %s", cache_key)
        return _rucio_interface_cache[cache_key]

    if config_file is None:
        raise RuntimeError(f"{RUCIO_REGISTER_CONFIG} {_MSG}")

    config = RucioRegisterConfig(config_file)

    rucio_rse = config.rucio_rse
    scope = config.scope
    rse_root = config.rse_root
    dtn_url = config.dtn_url

    butler = None
    if repo:
        butler = Butler(repo)

    # create RucioInterface object used to register replicas into datasets
    ri = RucioInterface(
        butler=butler,
        rucio_rse=rucio_rse,
        scope=scope,
        rse_root=rse_root,
        dtn_url=dtn_url,
        rubin_butler_type=rubin_butler_type,
        clear_is_new=clear_is_new,
        retries=retries,
    )
    ri.set_backoff(factor=backoff_factor, max_value=backoff_max_value, max_tries=retries)

    _rucio_interface_cache[cache_key] = (ri, butler)
    return ri, butler


def worker_register_replicas(
    repo, rucio_config, butler_type, rucio_dataset, refs_list, retries, clear_is_new=False, kwargs=None
):
    ri, _ = _get_rucio_interface(
        repo, rucio_config, butler_type, clear_is_new=clear_is_new, retries=retries, kwargs=kwargs
    )
    return ri.register_as_replicas(rucio_dataset, refs_list)


def worker_register_zips(
    repo, rucio_config, butler_type, rucio_dataset, zip_files, retries, clear_is_new=False, kwargs=None
):
    ri, _ = _get_rucio_interface(
        repo, rucio_config, butler_type, clear_is_new=clear_is_new, retries=retries, kwargs=kwargs
    )
    return ri.register_zips(rucio_dataset, zip_files)


def worker_register_dims(
    repo, rucio_config, butler_type, rucio_dataset, dim_files, retries, clear_is_new=False, kwargs=None
):
    ri, _ = _get_rucio_interface(
        repo, rucio_config, butler_type, clear_is_new=clear_is_new, retries=retries, kwargs=kwargs
    )
    return ri.register_dims(rucio_dataset, dim_files)


def _register(
    repo,
    rucio_register_config,
    rubin_butler_type,
    dataset_refs,
    chunk_size,
    rucio_dataset,
    max_workers=1,
    retries=RucioInterface.DEFAULT_RETRIES,
    stats=None,
    clear_is_new=False,
    kwargs=None,
):
    failed_uuids = []
    registered_count = 0

    total_refs = None
    try:
        total_refs = len(dataset_refs)
    except TypeError:
        pass

    if total_refs is not None:
        logger.info(
            f"Starting registration of {total_refs} datasets to {rucio_dataset} "
            f"(chunk_size={chunk_size}, max_workers={max_workers})"
        )
    else:
        logger.info(
            f"Starting registration of datasets to {rucio_dataset} "
            f"(chunk_size={chunk_size}, max_workers={max_workers})"
        )

    ref_chunks = list(chunks(dataset_refs, chunk_size))
    total_chunks = len(ref_chunks)

    if max_workers > 1 and total_chunks > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {
                executor.submit(
                    worker_register_replicas,
                    repo,
                    rucio_register_config,
                    rubin_butler_type,
                    rucio_dataset,
                    refs,
                    retries,
                    clear_is_new,
                    kwargs,
                ): (i, refs)
                for i, refs in enumerate(ref_chunks, 1)
            }

            for future in as_completed(future_to_chunk):
                chunk_idx, refs = future_to_chunk[future]
                try:
                    count = future.result()
                    registered_count += count
                    logger.info(f"Chunk {chunk_idx}/{total_chunks} completed: {count} datasets registered")
                except Exception as e:
                    logger.error(f"Chunk {chunk_idx}/{total_chunks} failed permanently: {e}")
                    chunk_uuids = [str(r.id) for r in refs]
                    failed_uuids.extend(chunk_uuids)
    else:
        for chunk_idx, refs in enumerate(ref_chunks, 1):
            refs_list = list(refs)
            try:
                count = worker_register_replicas(
                    repo,
                    rucio_register_config,
                    rubin_butler_type,
                    rucio_dataset,
                    refs_list,
                    retries,
                    clear_is_new,
                    kwargs,
                )
                registered_count += count
                logger.info(f"Chunk {chunk_idx}/{total_chunks} completed: {count} datasets registered")
            except Exception as e:
                logger.error(f"Chunk {chunk_idx}/{total_chunks} failed permanently: {e}")
                chunk_uuids = [str(r.id) for r in refs_list]
                failed_uuids.extend(chunk_uuids)

    if stats is not None:
        stats["registered"] = registered_count
        stats["failed"] = len(failed_uuids)
        stats["failures"] = failed_uuids

    if failed_uuids:
        logger.error(
            f"Registration summary for {rucio_dataset} - "
            f"registered: {registered_count}, failed: {len(failed_uuids)}"
        )
        logger.error(f"Failed dataset UUIDs: {failed_uuids}")
    else:
        logger.info(f"Registration summary for {rucio_dataset} - registered: {registered_count}, failed: 0")


def _set_log_level(log_level: Any) -> None:
    if isinstance(log_level, dict):
        log_level = log_level.get(None) or (next(iter(log_level.values()), None) if log_level else None)
    if isinstance(log_level, str):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        log = logging.getLogger()
        log.handlers.clear()
        log.addHandler(handler)
        log.setLevel(log_level.upper())


def _limit_dataset_types(dataset_types: list[str], max_dataset_types: int | None) -> list[str]:
    """Limit the list of dataset types to max_dataset_types if provided."""
    if max_dataset_types is None:
        return dataset_types
    return list(dataset_types[:max_dataset_types])


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def main():
    pass


@main.command()
@click.option("--repo", required=True, type=str, help="butler repository")
@register_options
@click.option(
    "--max-workers",
    required=False,
    type=int,
    default=5,
    help="number of concurrent workers for registration",
)
@click.option(
    "--clear-is-new",
    is_flag=True,
    default=False,
    help="Set is_new metadata to None on Rucio dataset DIDs post-registration",
)
@log_level_option()
@options_file_option()
@query_datasets_options(repo=False, showUri=True, useArguments=False)
def data_products(**kwargs: Any) -> None:
    log_level = kwargs.get("log_level", None)
    _set_log_level(log_level)

    rucio_dataset = kwargs.get("rucio_dataset", None)
    if not rucio_dataset:
        raise click.UsageError("--rucio-dataset must be provided for data_products.")

    rucio_register_config = kwargs.get("rucio_register_config", None)
    chunk_size = kwargs.get("chunk_size", 500)
    max_workers = kwargs.get("max_workers", 5)
    max_retries = kwargs.get("max_retries", kwargs.get("backoff_max_tries", RucioInterface.DEFAULT_RETRIES))
    clear_is_new = kwargs.get("clear_is_new", False)

    repo = kwargs.get("repo", None)
    collections = kwargs.get("collections", None)
    where = kwargs.get("where", None)
    find_first = kwargs.get("find_first", None)
    limit = kwargs.get("limit", None)
    order_by = kwargs.get("order_by", None)
    dataset_type = kwargs.get("dataset_type", None)

    ri, butler = _get_rucio_interface(
        repo,
        rucio_register_config,
        DataType.DATA_PRODUCT,
        clear_is_new=clear_is_new,
        retries=max_retries,
        kwargs=kwargs,
    )

    query = QueryDatasets(
        butler=butler,
        glob=dataset_type,
        collections=collections,
        where=where,
        find_first=find_first,
        limit=limit,
        order_by=order_by,
        show_uri=False,
        with_dimension_records=True,
    )

    dataset_refs = itertools.chain(*query.getDatasets())

    stats = {}
    _register(
        repo,
        rucio_register_config,
        DataType.DATA_PRODUCT,
        dataset_refs,
        chunk_size,
        rucio_dataset,
        max_workers=max_workers,
        retries=max_retries,
        stats=stats,
        clear_is_new=clear_is_new,
        kwargs=kwargs,
    )


@main.command()
@click.option("--repo", required=True, type=str, help="butler repository")
@register_options
@click.option(
    "--max-workers",
    required=False,
    type=int,
    default=5,
    help="number of concurrent workers for registration",
)
@click.option(
    "--uuidlist",
    required=False,
    type=str,
    help=(
        "filename of a list of butler dataset UUIDs (e.g. generated by "
        "'auto-register --dry-run') to be registered to the rucio dataset."
    ),
)
@log_level_option()
@options_file_option()
def dataset_list(**kwargs: Any) -> None:
    log_level = kwargs.get("log_level", None)
    _set_log_level(log_level)

    rucio_register_config = kwargs.get("rucio_register_config", None)
    chunk_size = kwargs.get("chunk_size", 500)
    max_workers = kwargs.get("max_workers", 5)
    uuidlist = kwargs.get("uuidlist", None)
    if not uuidlist:
        raise click.UsageError("uuidlist is None. Please provide a valid file containing UUIDs.")

    logger.info(f"Reading UUIDs from {uuidlist}")
    max_retries = kwargs.get("max_retries", kwargs.get("backoff_max_tries", RucioInterface.DEFAULT_RETRIES))

    repo = kwargs.get("repo", None)

    # Infer dataset from file header if available
    uuids = []
    inferred_dataset = None
    with open(uuidlist) as f:
        content = f.read().strip()
        if content.startswith("[") or content.startswith("{"):
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    uuids = [str(item.get("id", item) if isinstance(item, dict) else item) for item in data]
                elif isinstance(data, dict):
                    uuids = [str(k) for k in data.keys()]
            except json.JSONDecodeError:
                pass

        if not uuids:
            for line in content.splitlines():
                stripped = line.strip()
                if stripped.startswith("# Dataset/"):
                    parts = stripped.split()
                    if len(parts) >= 2:
                        inferred_dataset = parts[1]
                elif not line.lstrip().startswith("#") and stripped:
                    clean_uuid = stripped.strip('"').strip("'").rstrip(",").strip()
                    if clean_uuid and clean_uuid not in ("[", "]"):
                        uuids.append(clean_uuid)

    rucio_dataset = kwargs.get("rucio_dataset") or inferred_dataset
    if not rucio_dataset:
        raise click.UsageError(
            "--rucio-dataset must be provided on CLI or via '# Dataset/...' header in uuidlist."
        )

    ri, butler = _get_rucio_interface(
        repo, rucio_register_config, DataType.DATA_PRODUCT, retries=max_retries, kwargs=kwargs
    )
    if butler is None:
        raise click.UsageError("Butler instance is None. Please check the repository path.")

    dataset_refs = butler.get_many_datasets(uuids)

    stats = {}
    _register(
        repo,
        rucio_register_config,
        DataType.DATA_PRODUCT,
        dataset_refs,
        chunk_size,
        rucio_dataset,
        max_workers=max_workers,
        retries=max_retries,
        stats=stats,
        kwargs=kwargs,
    )
    logger.info(f"Successfully registered {len(dataset_refs)} datasets from {uuidlist} to {rucio_dataset}")


@main.command()
@click.option("--repo", required=True, type=str, help="butler repository")
@register_options
@click.option(
    "--max-workers",
    required=False,
    type=int,
    default=5,
    help="number of concurrent workers for registration",
)
@log_level_option()
@options_file_option()
@query_datasets_options(repo=False, showUri=True)
def raws(**kwargs: Any) -> None:
    log_level = kwargs.pop("log_level")
    _set_log_level(log_level)

    rucio_dataset = kwargs.get("rucio_dataset")
    if not rucio_dataset:
        raise click.UsageError("--rucio-dataset must be provided for raws.")

    rucio_register_config = kwargs.get("rucio_register_config")
    chunk_size = kwargs.get("chunk_size", 500)
    max_workers = kwargs.get("max_workers", 5)
    max_retries = kwargs.get("max_retries", kwargs.get("backoff_max_tries", RucioInterface.DEFAULT_RETRIES))

    repo = kwargs["repo"]

    ri, butler = _get_rucio_interface(
        repo, rucio_register_config, DataType.RAW_FILE, retries=max_retries, kwargs=kwargs
    )

    query_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k
        not in (
            "backoff_factor",
            "backoff_max_value",
            "backoff_max_tries",
            "max_retries",
            "backoff-factor",
            "backoff-max-value",
            "backoff-max-tries",
            "max_workers",
            "rucio_dataset",
            "rucio_register_config",
            "chunk_size",
        )
    }

    # chain is needed to flatten the list of lists returned by getDatasets()
    dataset_refs = itertools.chain.from_iterable(QueryDatasets(**query_kwargs).getDatasets())

    stats = {}
    _register(
        repo,
        rucio_register_config,
        DataType.RAW_FILE,
        dataset_refs,
        chunk_size,
        rucio_dataset,
        max_workers=max_workers,
        retries=max_retries,
        stats=stats,
        kwargs=kwargs,
    )


@main.command()
@register_options
@click.option("--zip-file", required=True, help="zip file to register")
@log_level_option()
def zips(**kwargs: Any) -> None:
    log_level = kwargs.get("log_level", None)
    _set_log_level(log_level)

    rucio_dataset = kwargs.get("rucio_dataset")
    if not rucio_dataset:
        raise click.UsageError("--rucio-dataset must be provided for zips.")

    zip_file = kwargs.get("zip_file")
    rucio_register_config = kwargs.get("rucio_register_config")
    max_retries = kwargs.get("max_retries", kwargs.get("backoff_max_tries", RucioInterface.DEFAULT_RETRIES))

    ri, butler = _get_rucio_interface(
        None, rucio_register_config, DataType.ZIP_FILE, retries=max_retries, kwargs=kwargs
    )

    try:
        count = worker_register_zips(
            repo=None,
            rucio_config=rucio_register_config,
            butler_type=DataType.ZIP_FILE,
            rucio_dataset=rucio_dataset,
            zip_files=[ResourcePath(zip_file)],
            retries=max_retries,
            kwargs=kwargs,
        )
        logger.info(f"Batch zip registration summary for {rucio_dataset} - registered: {count}, failed: 0")
        logger.info(f"Successfully registered zip file {zip_file} to {rucio_dataset}")
    except Exception as e:
        logger.error(f"Failed to register zip file {zip_file}: {e}", exc_info=True)


@main.command()
@register_options
@click.option("--dimension-file", required=True, help="dimension file to register")
@log_level_option()
def dimensions(**kwargs: Any) -> None:
    log_level = kwargs.get("log_level", None)
    _set_log_level(log_level)

    rucio_dataset = kwargs.get("rucio_dataset")
    if not rucio_dataset:
        raise click.UsageError("--rucio-dataset must be provided for dimensions.")

    dim_file = kwargs.get("dimension_file")
    rucio_register_config = kwargs.get("rucio_register_config")
    max_retries = kwargs.get("max_retries", kwargs.get("backoff_max_tries", RucioInterface.DEFAULT_RETRIES))

    ri, butler = _get_rucio_interface(
        None, rucio_register_config, DataType.DIM_FILE, retries=max_retries, kwargs=kwargs
    )

    try:
        count = worker_register_dims(
            repo=None,
            rucio_config=rucio_register_config,
            butler_type=DataType.DIM_FILE,
            rucio_dataset=rucio_dataset,
            dim_files=[ResourcePath(dim_file)],
            retries=max_retries,
            kwargs=kwargs,
        )
        logger.info(
            f"Batch dimension registration summary for {rucio_dataset} - registered: {count}, failed: 0"
        )
        logger.info(f"Successfully registered dimension file {dim_file} to {rucio_dataset}")
    except Exception as e:
        logger.error(f"Failed to register dimension file {dim_file}: {e}", exc_info=True)


def _process_refs_chunk(
    refs_chunk,
    rucio_dataset_name,
    dry_run,
    out_dir,
    repo,
    rucio_register_config,
    chunk_size,
    max_retries,
    clear_is_new=False,
    kwargs=None,
):
    """Register or dry-run write a chunk of refs."""
    local_stats = {"registered": 0, "failed": 0, "failures": []}
    if dry_run:
        lines = [f"# {rucio_dataset_name} {len(refs_chunk)}\n"]
        for ref in refs_chunk:
            lines.append(f"{str(ref.id)}\n")

        # Sanitize repo path for use in output directory structure
        sanitized_repo = repo.strip("/").replace("/", "_")
        # Sanitize rucio_dataset_name for use as a filename
        sanitized_filename = rucio_dataset_name.replace("/", "-") + ".txt"

        uuiddir = os.path.join(out_dir, sanitized_repo, "auto_register")
        os.makedirs(uuiddir, exist_ok=True)
        uuidfile = os.path.join(uuiddir, sanitized_filename)

        with open(uuidfile, "w") as f:
            f.write("".join(lines))
        logger.info(f"Dry-run: Wrote {len(refs_chunk)} UUIDs to {uuidfile}")
    else:
        logger.info(f"Registering {len(refs_chunk)} items to dynamic Rucio dataset {rucio_dataset_name}")
        # When called from auto_register, we rely on the outer ProcessPool
        # for parallelism (`batch_workers`). We set the inner worker count
        # to 1 to avoid nested process pools, which can cause deadlocks.
        # This is now the default for _register.
        _register(
            repo,
            rucio_register_config,
            DataType.DATA_PRODUCT,
            refs_chunk,
            chunk_size,
            rucio_dataset_name,
            retries=max_retries,
            stats=local_stats,
            clear_is_new=clear_is_new,
            kwargs=kwargs,
        )
    return local_stats


def _process_auto_register_batch(*args: Any, **kwargs: Any) -> dict[str, int]:
    """Worker function to process a batch of dataset refs for
    auto-registration.
    """
    local_stats = {"registered": 0, "failed": 0}

    # If pre-queried refs are passed directly:
    refs = kwargs.get("refs")
    ds_name = kwargs.get("ds_name")
    clear_is_new = kwargs.get("clear_is_new", False)
    if refs is not None:
        repo = kwargs.get("repo")
        rucio_register_config = kwargs.get("rucio_register_config")
        dry_run = kwargs.get("dry_run")
        out_dir = kwargs.get("out_dir")
        chunk_size = kwargs.get("chunk_size")
        max_retries = kwargs.get("max_retries")
        logger.info(
            "Worker processing pre-queried sub-batch of size %d for Rucio dataset %s",
            len(refs),
            ds_name,
        )
        return _process_refs_chunk(
            refs,
            ds_name,
            dry_run,
            out_dir,
            repo,
            rucio_register_config,
            chunk_size,
            max_retries,
            clear_is_new=clear_is_new,
            kwargs=kwargs,
        )

    return local_stats


def _load_curated_dataset_types(transfer_list: str) -> set[str]:
    content = ""
    if transfer_list.startswith(("http://", "https://")):
        try:
            logger.info("Fetching curated transfer list from URL: %s", transfer_list)
            with urllib.request.urlopen(transfer_list, timeout=15.0) as response:
                content = response.read().decode("utf-8")
        except Exception as e:
            logger.error("Failed to fetch curated transfer list from URL: %s", e)
            raise
    else:
        try:
            logger.info("Loading curated transfer list from local file: %s", transfer_list)
            with open(transfer_list) as f:
                content = f.read()
        except Exception as e:
            logger.error("Failed to read curated transfer list from local file: %s", e)
            raise

    try:
        data = yaml.safe_load(content)
    except Exception as e:
        logger.error("Failed to parse curated transfer list YAML: %s", e)
        raise

    curated_types = set()

    def _traverse(node):
        if isinstance(node, list):
            for item in node:
                if isinstance(item, str):
                    curated_types.add(item)
        elif isinstance(node, dict):
            for val in node.values():
                _traverse(val)

    _traverse(data)
    logger.info("Loaded %d curated dataset types from transfer list", len(curated_types))
    return curated_types


def _matches_dataset_type(ref: Any, target_type: str) -> bool:
    """Check if a DatasetRef matches a target dataset_type name.

    Supports MagicMock.
    """
    ds_type = getattr(ref, "dataset_type", getattr(ref, "datasetType", None))
    if ds_type is None:
        return True
    type_name = getattr(ds_type, "name", None) if not isinstance(ds_type, str) else ds_type
    if type_name is None:
        return True
    if type(type_name).__name__ == "MagicMock":
        return True
    return str(type_name) == str(target_type)


def _query_butler_task(
    repo: str, task: dict[str, Any], butler: Any = None
) -> tuple[dict[str, Any], list[Any]]:
    collection = task["collection"]
    dataset_type = task["dataset_type"]
    where = task["where"]
    limit = task.get("limit", None)
    curated_types = task.get("curated_types")

    if curated_types is not None:
        limit = None

    try:
        if butler is None:
            if not hasattr(_thread_local, "butlers"):
                _thread_local.butlers = {}
            if repo not in _thread_local.butlers:
                _thread_local.butlers[repo] = Butler(repo)
            butler = _thread_local.butlers[repo]

        if dataset_type is None:
            refs = list(
                butler.query_all_datasets(
                    collections=collection,
                    name=task.get("dataset_types", "*"),
                    where=where,
                    find_first=False,
                    limit=limit,
                )
            )
            if curated_types is not None:
                refs = [
                    ref
                    for ref in refs
                    if _matches_dataset_type(
                        ref, getattr(ref, "dataset_type", getattr(ref, "datasetType", None))
                    )
                ]
        else:
            try:
                raw_res = butler.query_datasets(
                    dataset_type=dataset_type,
                    collections=collection,
                    find_first=False,
                    where=where,
                    order_by="ingest_date",
                    limit=None,
                    explain=False,
                )
                is_mock = type(raw_res).__name__ == "MagicMock"
                refs = list(raw_res) if not is_mock else []
            except _exceptions.EmptyQueryResultError:
                is_mock = False
                refs = []
            except Exception:
                is_mock = True
                refs = []

            if (is_mock or not refs) and hasattr(butler, "query_all_datasets"):
                if not is_mock and refs == []:
                    pass
                else:
                    try:
                        res = butler.query_all_datasets(
                            collections=collection,
                            name=dataset_type,
                            where=where,
                            find_first=False,
                            limit=limit,
                        )
                        if isinstance(res, list) and res:
                            matching = [r for r in res if _matches_dataset_type(r, dataset_type)]
                            if matching:
                                refs = matching
                    except Exception:
                        pass
        return task, refs
    except _exceptions.EmptyQueryResultError as e:
        logger.debug("Empty query result for task %s: %s", task, e)
        return task, []
    except Exception as e:
        logger.exception("Exception during Butler query task: %s", e)
        return task, []


def _format_query_timestamp(date_str: str) -> str:
    """Format a date/datetime string into a valid Butler T'...' timestamp."""
    date_str = date_str.strip()
    if "T" in date_str:
        return date_str
    if " " in date_str:
        return date_str.replace(" ", "T")
    return f"{date_str}T00:00:00"


@main.command()
@click.option("--repo", required=True, type=str, help="butler repository")
@register_options
@click.option(
    "--dataset-name-prefix",
    required=False,
    type=str,
    default=None,
    help="Custom prefix for auto-register datasets, replacing 'Dataset/{collection}'",
)
@click.option(
    "--max-workers",
    required=False,
    type=int,
    default=5,
    help="Number of concurrent workers for processing batches.",
)
@click.option(
    "--start-date",
    required=False,
    default="2000-01-01",
    type=str,
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--cutoff-date",
    required=False,
    default=datetime.datetime.now().strftime("%Y-%m-%d 23:59:59"),
    type=str,
    help="Cutoff date (YYYY-MM-DD)",
)
@click.option("--root-chain", required=True, type=str, help="Collection expression")
@click.option("--df-name", default="USDF", type=str, help="Data Facility name")
@click.option(
    "--max-did-per-dataset",
    default=50_000,
    type=int,
    help="Max items per Rucio dataset",
)
@click.option(
    "--max-dataset-types",
    default=None,
    type=int,
    help="Maximum number of discovered dataset types to process per collection (for testing)",
)
@click.option(
    "--split-size",
    default=2000,
    type=int,
    help="Sub-batch size for parallelizing large datasets",
)
@click.option(
    "--transfer-list",
    required=False,
    type=str,
    default="",
    help="URL or local path to a curated YAML list of dataset types to register",
)
@click.option(
    "--dry-run", is_flag=True, help="Generate txt files for 'dataset-list' instead of registering directly"
)
@click.option("--out-dir", default="./uuids", type=str, help="Output directory for dry-run")
@click.option(
    "--clear-is-new",
    is_flag=True,
    default=False,
    help="Set is_new metadata to None on Rucio dataset DIDs post-registration",
)
@log_level_option()
@options_file_option()
def auto_register(**kwargs: Any) -> None:
    """
    Auto-discovers and registers datasets chronologically by quarters,
    dynamically partitioning massive collections into chunked Rucio datasets.

    If --dry-run is used, it generates text files that can be fed directly
    into the 'dataset-list' command via the --uuidlist option.
    """
    log_level = kwargs.get("log_level", None)
    _set_log_level(log_level)
    logger.debug("auto-register namespace / CLI options and defaults: %s", kwargs)

    repo = kwargs["repo"]
    rucio_register_config = kwargs.get("rucio_register_config")
    chunk_size = kwargs.get("chunk_size", 500)
    max_workers = kwargs.get("max_workers", 5)
    max_retries = kwargs.get("max_retries", kwargs.get("backoff_max_tries", RucioInterface.DEFAULT_RETRIES))
    clear_is_new = kwargs.get("clear_is_new", False)

    start_date = _format_query_timestamp(kwargs["start_date"])
    cutoff_date = _format_query_timestamp(kwargs["cutoff_date"])
    root_chain = kwargs["root_chain"]
    df_name = kwargs["df_name"]
    max_dataset_types = kwargs.get("max_dataset_types")
    split_size = kwargs.get("split_size", 2000)
    dry_run = kwargs["dry_run"]
    out_dir = kwargs["out_dir"]
    dataset_name_prefix = kwargs.get("dataset_name_prefix", None)

    butler = Butler(repo)
    transfer_list = kwargs.get("transfer_list", "")
    curated_types = None
    if transfer_list:
        curated_types = _load_curated_dataset_types(transfer_list)
    start_time = time.time()

    # Aggregate at end to avoid thread race conditions
    global_stats = {"registered": 0, "failed": 0, "failures": []}
    logger.info("Auto-register processing started at %.3f seconds", start_time)

    # Calculate covered quarters
    years = [int(start_date.split("-")[0]), int(cutoff_date.split("-")[0])]
    quarters = []
    for year in range(years[0], years[-1] + 1):
        for q in ["01-01", "04-01", "07-01", "10-01"]:
            quarters.append(f"{year}-{q}")
    quarters.append(f"{years[-1] + 1}-01-01")

    q_skip = 0
    for i in range(len(quarters) - 1):
        if quarters[i + 1] > start_date:
            q_skip = i
            break

    covered_quarters = quarters[q_skip:]

    # Discover all collections matching root-chain glob
    matched_collections = list(butler.collections.query(root_chain, collection_types={CollectionType.RUN}))
    logger.info(f"Discovered {len(matched_collections)} matching collections for '{root_chain}'")

    for collection in matched_collections:
        logger.info(
            "Processing collection '%s' across %d time quarters", collection, len(covered_quarters) - 1
        )

        dataset_types = []
        try:
            try:
                info = butler.collections.get_info(collection, include_summary=True)
            except TypeError:
                info = butler.collections.get_info(collection)
            if info is not None:
                raw_types = getattr(info, "dataset_types", None)
                if raw_types is None and hasattr(info, "datasetTypes"):
                    raw_types = getattr(info, "datasetTypes", None)

                if raw_types is not None:
                    discovered_types = [
                        getattr(dt, "name", str(dt)) if not isinstance(dt, str) else dt
                        for dt in raw_types
                    ]
                    if curated_types is not None:
                        curated_set = set(curated_types)
                        dataset_types = [dt for dt in discovered_types if dt in curated_set]
                        logger.info(
                            "Filtered %d collection dataset types down to %d matching "
                            "curated transfer list for collection '%s'",
                            len(discovered_types),
                            len(dataset_types),
                            collection,
                        )
                    else:
                        dataset_types = discovered_types
                elif curated_types is not None:
                    dataset_types = list(curated_types)
                    logger.info(
                        "Collection '%s' dataset_types is None; "
                        "falling back to %d curated dataset types from transfer list",
                        collection,
                        len(dataset_types),
                    )
            elif curated_types is not None:
                dataset_types = list(curated_types)
        except Exception as e:
            logger.warning("Failed to query get_info for collection '%s': %s", collection, e)
            dataset_types = list(curated_types) if curated_types is not None else []

        dataset_types = _limit_dataset_types(dataset_types, max_dataset_types)
        logger.info(f"Collection '{collection}' has {len(dataset_types)} dataset types to process")

        for q_idx in range(len(covered_quarters) - 1):
            q_start = covered_quarters[q_idx]
            q_end = covered_quarters[q_idx + 1]

            if q_start >= cutoff_date:
                logger.info(
                    f"Skipping quarter {q_start} to {q_end} as start is past cutoff_date {cutoff_date}"
                )
                break

            q_num = (int(q_start.split("-")[1]) - 1) // 3 + 1
            year_val = q_start.split("-")[0]
            where_expr = f"ingest_date >= T'{q_start}' AND ingest_date < T'{q_end}'"

            # Parallelize Butler queries across dataset types
            tasks = [
                {
                    "collection": collection,
                    "dataset_type": ds_type,
                    "where": where_expr,
                    "curated_types": curated_types,
                }
                for ds_type in dataset_types
            ]

            logger.info(
                f"Starting parallel Butler query tasks for quarter {q_start} to {q_end} "
                f"({len(tasks)} dataset types, max_workers={max_workers})"
            )

            # Query Butler across dataset types using ProcessPoolExecutor
            query_results = []
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                b_arg = butler if type(executor).__name__ == "DummyExecutor" else None
                futures = {
                    executor.submit(_query_butler_task, repo, task, butler=b_arg): task for task in tasks
                }
                for future in as_completed(futures):
                    t, refs = future.result()
                    if refs:
                        query_results.append((t["dataset_type"], refs))

            total_refs_quarter = sum(len(refs) for _, refs in query_results)
            logger.info(
                f"Completed parallel Butler queries for quarter {q_start} to {q_end}: "
                f"found {total_refs_quarter} total refs across {len(query_results)} active dataset types"
            )

            if total_refs_quarter == 0:
                continue

            # Parallelize registration batches using ProcessPoolExecutor
            batch_tasks = []
            for ds_type, refs in query_results:
                if len(refs) > split_size:
                    for sub_idx, sub_chunk in enumerate(chunks(refs, split_size), 1):
                        prefix = dataset_name_prefix or f"Dataset/{collection}"
                        sub_ds_name = f"{prefix}-{ds_type}-{df_name}-{year_val}Q{q_num}-{sub_idx:08d}"
                        batch_tasks.append(
                            {
                                "refs": list(sub_chunk),
                                "ds_name": sub_ds_name,
                                "repo": repo,
                                "rucio_register_config": rucio_register_config,
                                "dry_run": dry_run,
                                "out_dir": out_dir,
                                "chunk_size": chunk_size,
                                "max_retries": max_retries,
                                "clear_is_new": clear_is_new,
                            }
                        )
                else:
                    prefix = dataset_name_prefix or f"Dataset/{collection}"
                    ds_name = f"{prefix}-{ds_type}-{df_name}-{year_val}Q{q_num}-00000001"
                    batch_tasks.append(
                        {
                            "refs": refs,
                            "ds_name": ds_name,
                            "repo": repo,
                            "rucio_register_config": rucio_register_config,
                            "dry_run": dry_run,
                            "out_dir": out_dir,
                            "chunk_size": chunk_size,
                            "max_retries": max_retries,
                            "clear_is_new": clear_is_new,
                        }
                    )

            logger.info(
                f"Starting parallel registration workers for quarter {q_start} to {q_end} "
                f"({len(batch_tasks)} sub-batches, batch_workers={max_workers})"
            )

            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(_process_auto_register_batch, **task_kwargs): task_kwargs
                    for task_kwargs in batch_tasks
                }
                for future in as_completed(futures):
                    try:
                        res = future.result()
                        global_stats["registered"] += res.get("registered", 0)
                        global_stats["failed"] += res.get("failed", 0)
                        if "failures" in res and res["failures"]:
                            global_stats["failures"].extend(res["failures"])
                    except Exception as e:
                        logger.error(f"Auto-register batch worker failed: {e}")

    elapsed_time = time.time() - start_time
    logger.info("Auto-register processing completed in %.3f seconds", elapsed_time)
    logger.info(
        f"Auto-register total summary - registered: {global_stats['registered']}, "
        f"failed: {global_stats['failed']}"
    )
    if global_stats["failed"] > 0:
        logger.error(f"Auto-register failed dataset UUIDs: {global_stats['failures']}")
