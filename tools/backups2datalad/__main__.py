from __future__ import annotations

from functools import partial
import json
import logging
from pathlib import Path
import re
import shlex
import subprocess
import sys
from typing import AsyncIterable, Optional, Sequence

import anyio
import asyncclick as click
from click_loglevel import LogLevel
from dandi.consts import DANDISET_ID_REGEX
from dandi.dandiapi import DandiAPIClient
from datalad.api import Dataset

from .adataset import AsyncDataset
from .aioutil import TextProcess, aiter, pool_amap
from .config import Config
from .datasetter import DandiDatasetter
from .logging import log
from .util import format_errors, pdb_excepthook, quantify


@click.group()
@click.option(
    "-B",
    "--backup-root",
    type=click.Path(file_okay=False, path_type=Path),
)
@click.option(
    "-c",
    "--config",
    type=click.Path(dir_okay=False, exists=True, path_type=Path),
)
@click.option(
    "-J",
    "--jobs",
    type=int,
    help="How many parallel jobs to use when downloading and pushing",
)
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default="INFO",
    help="Set logging level  [default: INFO]",
)
@click.option("--pdb", is_flag=True, help="Drop into debugger if an error occurs")
@click.option(
    "--quiet-debug",
    is_flag=True,
    help="Log backups2datalad at DEBUG and all other loggers at INFO",
)
@click.pass_context
async def main(
    ctx: click.Context,
    jobs: Optional[int],
    log_level: int,
    pdb: bool,
    quiet_debug: bool,
    backup_root: Path,
    config: Optional[Path],
) -> None:
    if config is None:
        cfg = Config()
    else:
        cfg = Config.load_yaml(config)
    if backup_root is not None:
        cfg.backup_root = backup_root
    if jobs is not None:
        cfg.jobs = jobs
    ctx.obj = DandiDatasetter(
        dandi_client=DandiAPIClient.for_dandi_instance(cfg.dandi_instance),
        config=cfg,
    )
    if pdb:
        sys.excepthook = pdb_excepthook
    if quiet_debug:
        log.setLevel(logging.DEBUG)
        log_level = logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level,
    )
    await ctx.obj.debug_logfile()


@main.command()
@click.option(
    "--asset-filter",
    help="Only consider assets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "-f",
    "--force",
    type=click.Choice(["assets-update"]),
    help="Force all assets to be updated, even those whose metadata hasn't changed",
)
@click.option(
    "--tags/--no-tags",
    default=None,
    help="Enable/disable creation of tags for releases  [default: enabled]",
)
@click.argument("dandisets", nargs=-1)
@click.pass_obj
async def update_from_backup(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    exclude: Optional[re.Pattern[str]],
    tags: Optional[bool],
    asset_filter: Optional[re.Pattern[str]],
    force: Optional[str],
) -> None:
    async with datasetter:
        if asset_filter is not None:
            datasetter.config.asset_filter = asset_filter
        if force is not None:
            datasetter.config.force = force
        if tags is not None:
            datasetter.config.enable_tags = tags
        await datasetter.update_from_backup(dandisets, exclude=exclude)


@main.command()
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.argument("dandisets", nargs=-1)
@click.pass_obj
async def update_github_metadata(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    exclude: Optional[re.Pattern[str]],
) -> None:
    """
    Update the homepages and descriptions for the GitHub repositories for the
    given Dandisets.  If all Dandisets are updated, the description for the
    superdataset is set afterwards as well.

    `--target` must point to a clone of the superdataset in which every
    Dandiset subdataset is installed.
    """
    async with datasetter:
        await datasetter.update_github_metadata(dandisets, exclude=exclude)


@main.command()
@click.option(
    "--asset-filter",
    help="Only consider assets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "-f",
    "--force",
    type=click.Choice(["assets-update"]),
    help="Force all assets to be updated, even those whose metadata hasn't changed",
)
@click.option("--commitish", metavar="COMMITISH")
@click.option("--push/--no-push", default=True)
@click.argument("dandiset")
@click.argument("version")
@click.pass_obj
async def release(
    datasetter: DandiDatasetter,
    dandiset: str,
    version: str,
    commitish: Optional[str],
    push: bool,
    asset_filter: Optional[re.Pattern[str]],
    force: Optional[str],
) -> None:
    async with datasetter:
        if asset_filter is not None:
            datasetter.config.asset_filter = asset_filter
        if force is not None:
            datasetter.config.force = force
        dandiset_obj = await datasetter.dandi_client.aget_dandiset(dandiset, version)
        dataset = AsyncDataset(datasetter.config.dandiset_root / dandiset)
        await datasetter.mkrelease(
            dandiset_obj, dataset, commitish=commitish, push=push
        )
        if push:
            await dataset.push(to="github", jobs=datasetter.config.jobs)


@main.command("populate")
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option("-w", "--workers", type=int, help="Number of workers to run in parallel")
@click.argument("dandisets", nargs=-1)
@click.pass_obj
async def populate_cmd(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    exclude: Optional[re.Pattern[str]],
    workers: Optional[int],
) -> None:
    async with datasetter:
        if (r := datasetter.config.dandisets.remote) is not None:
            backup_remote = r.name
        else:
            raise click.UsageError("dandisets.remote not set in config file")
        if workers is not None:
            datasetter.config.workers = workers
        if dandisets:
            diriter = (datasetter.config.dandiset_root / d for d in dandisets)
        else:
            diriter = datasetter.config.dandiset_root.iterdir()
        dirs: list[Path] = []
        for p in diriter:
            if p.is_dir() and re.fullmatch(DANDISET_ID_REGEX, p.name):
                if exclude is not None and exclude.search(p.name):
                    log.debug("Skipping dandiset %s", p.name)
                else:
                    dirs.append(p)
            else:
                log.debug("Skipping non-Dandiset node %s", p.name)
        report = await pool_amap(
            partial(
                populate,
                backup_remote=backup_remote,
                pathtype="Dandiset",
                jobs=datasetter.config.jobs,
            ),
            afilter_installed(dirs),
            workers=datasetter.config.workers,
        )
        if report.failed:
            sys.exit(f"{quantify(len(report.failed), 'populate job')} failed")


@main.command()
@click.option("-w", "--workers", type=int, help="Number of workers to run in parallel")
@click.argument("zarrs", nargs=-1)
@click.pass_obj
async def populate_zarrs(
    datasetter: DandiDatasetter, zarrs: Sequence[str], workers: Optional[int]
) -> None:
    async with datasetter:
        zcfg = datasetter.config.zarrs
        if zcfg is None:
            raise click.UsageError("Zarr backups not configured in config file")
        if (r := zcfg.remote) is not None:
            backup_remote = r.name
        else:
            raise click.UsageError("zarrs.remote not set in config file")
        if workers is not None:
            datasetter.config.workers = workers
        zarr_root = datasetter.config.zarr_root
        assert zarr_root is not None
        if zarrs:
            diriter = (zarr_root / z for z in zarrs)
        else:
            diriter = zarr_root.iterdir()
        dirs: list[Path] = []
        for p in diriter:
            if p.is_dir() and p.name not in (".git", ".datalad"):
                dirs.append(p)
            else:
                log.debug("Skipping non-Zarr node %s", p.name)
        report = await pool_amap(
            partial(
                populate,
                backup_remote=backup_remote,
                pathtype="Zarr",
                jobs=datasetter.config.jobs,
            ),
            afilter_installed(dirs),
            workers=datasetter.config.workers,
        )
        if report.failed:
            sys.exit(f"{quantify(len(report.failed), 'populate-zarr job')} failed")


async def populate(dirpath: Path, backup_remote: str, pathtype: str, jobs: int) -> None:
    desc = f"{pathtype} {dirpath.name}"
    log.info("Downloading files for %s", desc)
    await call_annex_json(
        "get",
        "-c",
        "annex.retry=3",
        "--jobs",
        str(jobs),
        "--from=web",
        "--not",
        "--in",
        backup_remote,
        "--and",
        "--not",
        "--in",
        "here",
        path=dirpath,
    )
    i = 0
    while True:
        log.info("Moving files for %s to backup remote", desc)
        try:
            await call_annex_json(
                "move",
                "-c",
                "annex.retry=3",
                "--jobs",
                str(jobs),
                "--to",
                backup_remote,
                path=dirpath,
            )
        except RuntimeError as e:
            i += 1
            if i < 5:
                log.error("%s; retrying", e)
                continue
            else:
                raise
        else:
            break


async def call_annex_json(cmd: str, *args: str, path: Path) -> None:
    cmd_full = ["git-annex", cmd, *args, "--json", "--json-error-messages"]
    log.debug("Running %s", shlex.join(cmd_full))
    success = 0
    failed = 0
    async with await anyio.open_process(
        cmd_full,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=None,
        cwd=path,
    ) as p0, TextProcess(p0, name=cmd) as p:
        async for line in aiter(p):
            data = json.loads(line)
            if data["success"]:
                success += 1
            else:
                log.error(
                    "`git-annex %s` failed for %s:%s",
                    cmd,
                    data["file"],
                    format_errors(data["error-messages"]),
                )
                failed += 1
    log.info(
        "git-annex %s: %s succeeded, %s failed",
        cmd,
        quantify(success, "file"),
        quantify(failed, "file"),
    )
    if failed:
        raise RuntimeError(f"git-annex {cmd} failed for {quantify(failed, 'file')}")


async def afilter_installed(datasets: list[Path]) -> AsyncIterable[Path]:
    for p in datasets:
        ds = Dataset(p)
        if not ds.is_installed():
            log.info("Dataset %s is not installed; skipping", p.name)
        else:
            yield p


if __name__ == "__main__":
    main(_anyio_backend="asyncio")
