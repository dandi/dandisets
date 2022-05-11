from __future__ import annotations

import logging
from pathlib import Path
import re
import subprocess
import sys
from typing import Optional, Sequence

import click
from click_loglevel import LogLevel
from dandi.consts import DANDISET_ID_REGEX, known_instances
from dandi.dandiapi import DandiAPIClient
from datalad.api import Dataset

from .datasetter import DandiDatasetter
from .util import Config, log, pdb_excepthook


@click.group()
@click.option(
    "--asset-filter",
    help="Only consider assets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "-i",
    "--dandi-instance",
    type=click.Choice(sorted(known_instances)),
    default="dandi",
    help="DANDI instance to use",
    show_default=True,
)
@click.option(
    "-J",
    "--jobs",
    type=int,
    default=10,
    help="How many parallel jobs to use when downloading and pushing",
    show_default=True,
)
@click.option(
    "-f",
    "--force",
    type=click.Choice(["assets-update"]),
    help="Force all assets to be updated, even those whose metadata hasn't changed",
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
@click.option(
    "--s3bucket",
    help="S3 bucket on which the Dandisets' assets are stored",
    default="dandiarchive",
    show_default=True,
)
@click.option(
    "-T",
    "--target",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path(),
)
@click.pass_context
def main(
    ctx: click.Context,
    asset_filter: Optional[re.Pattern[str]],
    dandi_instance: str,
    force: Optional[str],
    jobs: int,
    log_level: int,
    pdb: bool,
    quiet_debug: bool,
    target: Path,
    s3bucket: str,
) -> None:
    ctx.obj = DandiDatasetter(
        dandi_client=ctx.with_resource(
            DandiAPIClient.for_dandi_instance(dandi_instance)
        ),
        target_path=target,
        config=Config(
            asset_filter=asset_filter,
            jobs=jobs,
            force=force,
            s3bucket=s3bucket,
        ),
    )
    if pdb:
        sys.excepthook = pdb_excepthook
    if quiet_debug:
        log.setLevel(logging.DEBUG)
        log_level = logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level,
        force=True,  # Override dandi's settings
    )
    ctx.obj.debug_logfile()


@main.command()
@click.option("--backup-remote", help="Name of the rclone remote to push to")
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "--gh-org", help="GitHub organization to create Dandiset repositories under"
)
@click.option(
    "--tags/--no-tags",
    default=True,
    help="Enable/disable creation of tags for releases  [default: enabled]",
)
@click.option("--zarr-backup-remote", help="Name of the rclone remote to push Zarrs to")
@click.option(
    "--zarr-gh-org", help="GitHub organization to create Zarr repositories under"
)
@click.option(
    "-Z",
    "--zarr-target",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
)
@click.argument("dandisets", nargs=-1)
@click.pass_obj
def update_from_backup(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    backup_remote: Optional[str],
    zarr_backup_remote: Optional[str],
    gh_org: Optional[str],
    zarr_gh_org: Optional[str],
    zarr_target: Path,
    exclude: Optional[re.Pattern[str]],
    tags: bool,
) -> None:
    if (gh_org is None) != (zarr_gh_org is None):
        raise click.UsageError("--gh-org and --zarr-gh-org must be defined together")
    datasetter.config.backup_remote = backup_remote
    datasetter.config.zarr_backup_remote = zarr_backup_remote
    datasetter.config.enable_tags = tags
    datasetter.config.gh_org = gh_org
    datasetter.config.zarr_gh_org = zarr_gh_org
    datasetter.config.zarr_target = zarr_target
    datasetter.update_from_backup(dandisets, exclude=exclude)


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
def update_github_metadata(
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
    datasetter.update_github_metadata(dandisets, exclude=exclude)


@main.command()
@click.option("--commitish", metavar="COMMITISH")
@click.option("--push/--no-push", default=True)
@click.argument("dandiset")
@click.argument("version")
@click.pass_obj
def release(
    datasetter: DandiDatasetter,
    dandiset: str,
    version: str,
    commitish: Optional[str],
    push: bool,
) -> None:
    dandiset_obj = datasetter.dandi_client.get_dandiset(dandiset, version)
    dataset = Dataset(datasetter.target_path / dandiset)
    datasetter.mkrelease(dandiset_obj, dataset, commitish=commitish, push=push)
    if push:
        dataset.push(to="github", jobs=datasetter.config.jobs)


@main.command("populate")
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.argument("backup_remote")
@click.argument("dandisets", nargs=-1)
@click.pass_obj
def populate_cmd(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    backup_remote: str,
    exclude: Optional[re.Pattern[str]],
) -> None:
    if dandisets:
        dirs = [datasetter.target_path / d for d in dandisets]
    else:
        dirs = list(datasetter.target_path.iterdir())
    for p in dirs:
        if p.is_dir() and re.fullmatch(DANDISET_ID_REGEX, p.name):
            if exclude is not None and exclude.search(p.name):
                log.debug("Skipping dandiset %s", p.name)
            elif not Dataset(p).is_installed():
                log.info("Dataset %s is not installed; skipping", p.name)
            else:
                populate(p, backup_remote, f"Dandiset {p.name}", datasetter.config.jobs)
        else:
            log.debug("Skipping non-Dandiset node %s", p.name)


@main.command()
@click.option(
    "-Z",
    "--zarr-target",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
)
@click.argument("backup_remote")
@click.argument("zarrs", nargs=-1)
@click.pass_obj
def populate_zarrs(
    datasetter: DandiDatasetter,
    zarr_target: Path,
    zarrs: Sequence[str],
    backup_remote: str,
) -> None:
    if zarrs:
        dirs = [zarr_target / z for z in zarrs]
    else:
        dirs = list(zarr_target.iterdir())
    for p in dirs:
        if p.is_dir() and p.name not in (".git", ".datalad"):
            if not Dataset(p).is_installed():
                log.info("Zarr %s is not installed; skipping", p.name)
            else:
                populate(p, backup_remote, f"Zarr {p.name}", datasetter.config.jobs)
        else:
            log.debug("Skipping non-Zarr node %s", p.name)


def populate(dirpath: Path, backup_remote: str, desc: str, jobs: int) -> None:
    log.info("Downloading files for %s", desc)
    subprocess.run(
        [
            "git-annex",
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
        ],
        check=True,
        cwd=dirpath,
    )
    log.info("Moving files for %s to backup remote", desc)
    subprocess.run(
        [
            "git-annex",
            "move",
            "-c",
            "annex.retry=3",
            "--jobs",
            str(jobs),
            "--to",
            backup_remote,
        ],
        check=True,
        cwd=dirpath,
    )


if __name__ == "__main__":
    main()
