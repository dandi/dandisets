from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
import logging
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
from types import TracebackType
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Type, Union, cast
from urllib.parse import urlparse, urlunparse

import boto3
from botocore import UNSIGNED
from botocore.client import Config as BotoConfig
from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import RemoteAsset, RemoteDandiset
from dandi.dandiset import APIDandiset
from datalad.api import Dataset
from morecontext import envset

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from . import log


@dataclass
class Config:
    ignore_errors: bool = False
    asset_filter: Optional[re.Pattern[str]] = None
    jobs: int = 10
    force: Optional[str] = None
    content_url_regex: str = r"amazonaws.com/.*blobs/"
    s3bucket: str = "dandiarchive"
    enable_tags: bool = True
    backup_remote: Optional[str] = None

    def match_asset(self, asset_path: str) -> bool:
        return bool(self.asset_filter is None or self.asset_filter.search(asset_path))

    @cached_property
    def s3client(self) -> "S3Client":
        return boto3.client("s3", config=BotoConfig(signature_version=UNSIGNED))

    def get_file_bucket_url(self, asset: RemoteAsset) -> str:
        log.debug("Fetching bucket URL for asset")
        aws_url = asset.get_content_url(self.content_url_regex)
        urlbits = urlparse(aws_url)
        log.debug("About to query S3")
        s3meta = self.s3client.get_object(
            Bucket=self.s3bucket, Key=urlbits.path.lstrip("/")
        )
        log.debug("Got bucket URL for asset")
        return urlunparse(urlbits._replace(query=f"versionId={s3meta['VersionId']}"))


@contextmanager
def custom_commit_date(dt: Optional[datetime]) -> Iterator[None]:
    if dt is not None:
        with envset("GIT_AUTHOR_NAME", "DANDI User"):
            with envset("GIT_AUTHOR_EMAIL", "info@dandiarchive.org"):
                with envset("GIT_AUTHOR_DATE", str(dt)):
                    yield
    else:
        yield


def dataset_files(dspath: Path) -> Iterator[str]:
    files = deque(
        p
        for p in dspath.iterdir()
        if p.name
        not in (".dandi", ".datalad", ".git", ".gitattributes", dandiset_metadata_file)
    )
    while files:
        p = files.popleft()
        if p.is_file():
            yield str(p.relative_to(dspath))
        elif p.is_dir():
            files.extend(p.iterdir())


@contextmanager
def dandi_logging(dandiset_path: Path) -> Iterator[Path]:
    logdir = dandiset_path / ".git" / "dandi" / "logs"
    logdir.mkdir(exist_ok=True, parents=True)
    filename = "sync-{:%Y%m%d%H%M%SZ}-{}.log".format(datetime.utcnow(), os.getpid())
    logfile = logdir / filename
    handler = logging.FileHandler(logfile, encoding="utf-8")
    fmter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handler.setFormatter(fmter)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        yield logfile
    except Exception:
        log.exception("Operation failed with exception:")
        raise
    finally:
        root.removeHandler(handler)


def is_interactive() -> bool:
    """Return True if all in/outs are tty"""
    return sys.stdin.isatty() and sys.stdout.isatty() and sys.stderr.isatty()


def pdb_excepthook(
    exc_type: Type[BaseException], exc_value: BaseException, tb: TracebackType
) -> None:
    import traceback

    traceback.print_exception(exc_type, exc_value, tb)
    print()
    if is_interactive():
        import pdb

        pdb.post_mortem(tb)


def asset2dict(asset: RemoteAsset) -> Dict[str, Any]:
    return {**asset.json_dict(), "metadata": asset.get_raw_metadata()}


def assets_eq(remote_assets: List[RemoteAsset], local_assets: List[dict]) -> bool:
    return {a.identifier: asset2dict(a) for a in remote_assets} == {
        a["asset_id"]: a for a in local_assets
    }


def readcmd(*args: Union[str, Path], **kwargs: Any) -> str:
    log.debug("Running: %s", shlex.join(map(str, args)))
    return cast(str, subprocess.check_output(args, text=True, **kwargs)).strip()


def update_dandiset_metadata(dandiset: RemoteDandiset, ds: Dataset) -> None:
    log.info("Updating metadata file")
    (ds.pathobj / dandiset_metadata_file).unlink(missing_ok=True)
    metadata = dandiset.get_raw_metadata()
    APIDandiset(ds.pathobj, allow_empty=True).update_metadata(metadata)
    ds.repo.add([dandiset_metadata_file])


def quantify(qty: int, singular: str, plural: Optional[str] = None) -> str:
    if qty == 1:
        return f"{qty} {singular}"
    elif plural is None:
        return f"{qty} {singular}s"
    else:
        return f"{qty} {plural}"
