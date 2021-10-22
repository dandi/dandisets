from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
from types import TracebackType
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import RemoteAsset, RemoteDandiset
from dandi.dandiset import APIDandiset
from datalad.api import Dataset
from datalad.support.json_py import dump
from morecontext import envset
import trio

from . import log

if sys.version_info[:2] >= (3, 10):
    # So aiter() can be re-exported without mypy complaining:
    from builtins import aiter as aiter
    from contextlib import aclosing
else:
    from async_generator import aclosing

    T = TypeVar("T")

    def aiter(obj: AsyncIterable[T]) -> AsyncIterator[T]:
        return obj.__aiter__()

    async def anext(obj: AsyncIterator[T]) -> T:
        return await obj.__anext__()


DEEP_DEBUG = 5


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


@dataclass
class Report:
    added: int = 0
    updated: int = 0
    downloaded: int = 0
    failed: int = 0
    hash_mismatches: int = 0
    old_unhashed: int = 0

    def check(self) -> None:
        errors: List[str] = []
        if self.failed:
            errors.append(f"{quantify(self.failed, 'assets')} failed to download")
        if self.hash_mismatches:
            errors.append(
                f"{quantify(self.hash_mismatches, 'assets')} had the wrong hash"
                " after downloading"
            )
        if self.old_unhashed:
            errors.append(
                f"{quantify(self.old_unhashed, 'assets')} on server had no"
                " SHA256 hash despite advanced age"
            )
        if errors:
            raise RuntimeError("Errors occured while downloading: {'; '.join(errors)}")


@dataclass
class TextProcess(trio.abc.AsyncResource):
    p: trio.Process
    name: str
    encoding: str = "utf-8"
    lineiter: Optional[AsyncIterator[str]] = field(init=False, default=None)

    async def aclose(self) -> None:
        if self.lineiter is not None:
            await self.lineiter.aclose()  # type: ignore[attr-defined]
        await self.p.aclose()
        if self.p.returncode not in (None, 0):
            log.warning(
                "git-annex %s command exited with return code %d",
                self.name,
                self.p.returncode,
            )

    async def send(self, s: str) -> None:
        assert self.p.stdin is not None
        log.log(DEEP_DEBUG, "Sending to %s command: %r", self.name, s)
        await self.p.stdin.send_all(s.encode(self.encoding))

    async def readline(self) -> str:
        if self.lineiter is None:
            self.lineiter = aiter(self)
        try:
            return await anext(self.lineiter)
        except StopAsyncIteration:
            return ""

    async def __aiter__(self) -> AsyncIterator[str]:
        def decode(bs: bytes) -> str:
            s = bs.decode(self.encoding)
            log.log(DEEP_DEBUG, "Decoded line from %s command: %r", self.name, s)
            return s

        buff = b""
        assert self.p.stdout is not None
        async with aclosing(aiter(self.p.stdout)) as blobiter:  # type: ignore[type-var]
            async for blob in blobiter:
                log.log(DEEP_DEBUG, "Read from %s command: %r", self.name, blob)
                lines, buff = split_unix_lines(buff + blob)
                for ln in lines:
                    yield decode(ln)
        if buff:
            lines, buff = split_unix_lines(buff)
            for ln in lines:
                yield decode(ln)
            if buff:
                yield decode(buff)


@dataclass
class AssetTracker:
    #: Paths of files found when the syncing started, minus the paths for any
    #: assets downloaded during syncing
    local_assets: Set[str]
    #: Metadata of assets downloaded during the sync
    asset_metadata: List[dict] = field(init=False, default_factory=list)
    #: Asset metadata from previous sync, as a mapping from asset paths to
    #: metadata
    saved_metadata: Dict[str, dict]
    #: Paths of assets that are not being downloaded this run due to a lack of
    #: SHA256 digests
    future_assets: Set[str] = field(init=False, default_factory=set)

    @classmethod
    def from_dataset(cls, dspath: Path) -> AssetTracker:
        local_assets = set(dataset_files(dspath))
        saved_metadata: Dict[str, dict] = {}
        try:
            with (dspath / ".dandi" / "assets.json").open() as fp:
                for md in json.load(fp):
                    if isinstance(md, str):
                        # Old version of assets.json; ignore
                        pass
                    else:
                        saved_metadata[md["path"].lstrip("/")] = md
        except FileNotFoundError:
            pass
        return cls(local_assets=local_assets, saved_metadata=saved_metadata)

    def register_asset(self, asset: RemoteAsset, force: Optional[str]) -> bool:
        # Returns True if the asset's metadata is unchanged since the last sync
        self.local_assets.discard(asset.path)
        adict = asset2dict(asset)
        self.asset_metadata.append(adict)
        return force != "check" and adict == self.saved_metadata.get(asset.path)

    def mark_future(self, asset: RemoteAsset) -> None:
        self.future_assets.add(asset.path)

    def get_deleted(self, config: Config) -> Iterator[str]:
        """
        Yields paths of deleted assets while also adding saved metadata for
        future assets to the tracked asset metadata
        """
        for apath in self.local_assets:
            if not config.match_asset(apath):
                pass
            elif apath in self.future_assets:
                try:
                    self.asset_metadata.append(self.saved_metadata[apath])
                except KeyError:
                    pass
            else:
                yield apath

    def dump(self, dspath: Path) -> None:
        dump(self.asset_metadata, dspath / ".dandi" / "assets.json")

    @property
    def future_qty(self) -> int:
        return len(self.future_assets)


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


def key2hash(key: str) -> str:
    return key.split("-")[-1].partition(".")[0]


async def open_git_annex(*args: str, path: Optional[Path] = None) -> TextProcess:
    # Note: The syntax for starting an interactable process will change in trio
    # 0.20.0.
    log.debug("Running git-annex %s", shlex.join(args))
    p = await trio.open_process(
        ["git-annex", *args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        cwd=str(path),  # trio-typing says this has to be a string.
    )
    return TextProcess(p, name=args[0])


# We can't use splitlines() because it splits on \r, but we only want to split
# on \n.
def split_unix_lines(bs: bytes) -> Tuple[List[bytes], bytes]:
    lines: List[bytes] = []
    while True:
        try:
            i = bs.index(b"\n")
        except ValueError:
            break
        lines.append(bs[: i + 1])
        bs = bs[i + 1 :]
    return lines, bs
