from __future__ import annotations

import logging
from pathlib import Path
import re
import sys
from typing import Optional, Sequence

import click
from click_loglevel import LogLevel
from dandi.consts import known_instances
from dandi.dandiapi import DandiAPIClient
from datalad.api import Dataset

from . import log
from .datasetter import DandiDatasetter
from .util import Config, pdb_excepthook


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
@click.option("-f", "--force", type=click.Choice(["check"]))
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
    "-T", "--target", type=click.Path(file_okay=False, path_type=Path), required=True
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


@main.command()
@click.option("--backup-remote", help="Name of the rclone remote to push to")
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option("--gh-org", help="GitHub organization to create repositories under")
@click.option(
    "--tags/--no-tags",
    default=True,
    help="Enable/disable creation of tags for releases  [default: enabled]",
)
@click.argument("dandisets", nargs=-1)
@click.pass_obj
def update_from_backup(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    backup_remote: Optional[str],
    gh_org: Optional[str],
    exclude: Optional[re.Pattern[str]],
    tags: bool,
) -> None:
    datasetter.config.backup_remote = backup_remote
    datasetter.config.enable_tags = tags
    datasetter.update_from_backup(dandisets, exclude=exclude, gh_org=gh_org)


@main.command()
@click.option(
    "-e",
    "--exclude",
    help="Skip dandisets matching the given regex",
    metavar="REGEX",
    type=re.compile,
)
@click.option(
    "--gh-org",
    help="GitHub organization under which repositories reside",
    required=True,
)
@click.argument("dandisets", nargs=-1)
@click.pass_obj
def update_github_metadata(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    exclude: Optional[re.Pattern[str]],
    gh_org: str,
) -> None:
    datasetter.update_github_metadata(dandisets, exclude=exclude, gh_org=gh_org)


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


if __name__ == "__main__":
    main()
