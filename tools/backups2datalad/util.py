from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import shlex
import subprocess
import sys
import textwrap
import threading
from types import TracebackType
from typing import Any, Dict, Iterator, List, Optional, Set, Type, Union, cast

from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import RemoteAsset, RemoteDandiset
from dandi.dandiset import APIDandiset
import datalad
from datalad.api import Dataset
from datalad.support.json_py import dump
from morecontext import envset

from .config import Config, Remote
from .consts import DEFAULT_BRANCH

log = logging.getLogger("backups2datalad")


@dataclass
class Report:
    commits: int = 0
    added: int = 0
    updated: int = 0
    registered: int = 0
    downloaded: int = 0
    failed: int = 0
    hash_mismatches: int = 0
    old_unhashed: int = 0

    def update(self, other: Report) -> None:
        self.commits += other.commits
        self.added += other.added
        self.updated += other.updated
        self.registered += other.registered
        self.downloaded += other.downloaded
        self.failed += other.failed
        self.hash_mismatches += other.hash_mismatches
        self.old_unhashed += other.old_unhashed

    def get_commit_message(self) -> str:
        msgparts = []
        if self.added:
            msgparts.append(f"{quantify(self.added, 'file')} added")
        if self.updated:
            msgparts.append(f"{quantify(self.updated, 'file')} updated")
        if not msgparts:
            msgparts.append("Only some metadata updates")
        return f"[backups2datalad] {', '.join(msgparts)}"

    def check(self) -> None:
        errors: List[str] = []
        if self.failed:
            errors.append(f"{quantify(self.failed, 'asset')} failed to download")
        if self.hash_mismatches:
            errors.append(
                f"{quantify(self.hash_mismatches, 'asset')} had the wrong hash"
                " after downloading"
            )
        if self.old_unhashed:
            errors.append(
                f"{quantify(self.old_unhashed, 'asset')} on server had no"
                " SHA256 hash despite advanced age"
            )
        if errors:
            raise RuntimeError(
                f"Errors occurred while downloading: {'; '.join(errors)}"
            )


@dataclass
class AssetTracker:
    #: The path to the .dandi/assets.json file that this instance manages
    filepath: Path
    #: Paths of files found when the syncing started, minus the paths for any
    #: assets downloaded during syncing
    local_assets: Set[str]
    #: Metadata for assets currently being downloaded, as a mapping from asset
    #: paths to metadata
    in_progress: Dict[str, dict] = field(init=False, default_factory=dict)
    #: Asset metadata from previous sync, plus metadata for any assets
    #: completely downloaded during the sync, as a mapping from asset paths to
    #: metadata
    asset_metadata: Dict[str, dict]
    #: Paths of assets that are not being downloaded this run due to a lack of
    #: SHA256 digests
    future_assets: Set[str] = field(init=False, default_factory=set)

    @classmethod
    def from_dataset(cls, dspath: Path) -> AssetTracker:
        filepath = dspath / ".dandi" / "assets.json"
        local_assets = set(dataset_files(dspath))
        asset_metadata: Dict[str, dict] = {}
        try:
            with filepath.open() as fp:
                for md in json.load(fp):
                    if isinstance(md, str):
                        raise RuntimeError(f"Old assets.json format found in {dspath}")
                    else:
                        asset_metadata[md["path"].lstrip("/")] = md
        except FileNotFoundError:
            pass
        return cls(
            filepath=filepath, local_assets=local_assets, asset_metadata=asset_metadata
        )

    def register_asset(self, asset: RemoteAsset, force: Optional[str]) -> bool:
        # Returns True if the asset's metadata has changed (or if we should act
        # like it's changed) since the last sync
        self.local_assets.discard(asset.path)
        adict = asset2dict(asset)
        self.in_progress[asset.path] = adict
        return adict != self.asset_metadata.get(asset.path) or force == "assets-update"

    def finish_asset(self, asset_path: str) -> None:
        self.asset_metadata[asset_path] = self.in_progress.pop(asset_path)

    def mark_future(self, asset: RemoteAsset) -> None:
        self.future_assets.add(asset.path)

    def get_deleted(self, config: Config) -> Iterator[str]:
        """
        Yields paths of deleted assets and removes their metadata from
        `asset_metadata`
        """
        for apath in self.local_assets:
            if config.match_asset(apath):
                self.asset_metadata.pop(apath, None)
                yield apath

    def dump(self) -> None:
        self.filepath.parent.mkdir(exist_ok=True, parents=True)
        dump([md for _, md in sorted(self.asset_metadata.items())], self.filepath)

    @property
    def future_qty(self) -> int:
        return len(self.future_assets)


@contextmanager
def custom_commit_date(
    dt: Optional[datetime],
    lock: threading.Lock = threading.Lock(),  # noqa: B008
) -> Iterator[None]:
    if dt is not None:
        with lock:
            # Lock in order to avoid issues from multiple
            # custom_commit_date()'s being in scope at once
            with envset("GIT_AUTHOR_NAME", "DANDI User"):
                with envset("GIT_AUTHOR_EMAIL", "info@dandiarchive.org"):
                    with envset("GIT_AUTHOR_DATE", str(dt)):
                        yield
    else:
        yield


def dataset_files(dspath: Path) -> Iterator[str]:
    files = deque(
        p for p in dspath.iterdir() if not is_meta_file(p.name, dandiset=True)
    )
    while files:
        p = files.popleft()
        if p.is_file() or p.is_symlink():
            yield str(p.relative_to(dspath))
        elif p.is_dir():
            if (p / ".git").exists():
                yield str(p.relative_to(dspath))
            else:
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
    exc_type: Type[BaseException], exc_value: BaseException, tb: Optional[TracebackType]
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


def format_errors(messages: List[str]) -> str:
    if not messages:
        return " <no error message>"
    elif len(messages) == 1:
        return " " + messages[0]
    else:
        return "\n\n" + textwrap.indent("".join(messages), " " * 4) + "\n"


def exp_wait(
    base: float = 1.25,
    multiplier: float = 1,
    attempts: Optional[int] = None,
) -> Iterator[float]:
    """
    Returns a generator of values usable as `sleep()` times when retrying
    something with exponential backoff.

    :param float base:
    :param float multiplier: value to multiply values by after exponentiation
    :param Optional[int] attempts: how many values to yield; set to `None` to
        yield forever
    :rtype: Iterator[float]
    """
    n = 0
    while attempts is None or n < attempts:
        yield base**n * multiplier
        n += 1


def init_dataset(
    ds: Dataset,
    desc: str,
    commit_date: datetime,
    backup_remote: Optional[Remote] = None,
    backend: str = "SHA256E",
    cfg_proc: Optional[str] = "text2git",
) -> None:
    log.info("Creating dataset for %s", desc)
    try:
        datalad.cfg.set("datalad.repo.backend", backend, scope="override")
        with custom_commit_date(commit_date):
            with envset(
                "GIT_CONFIG_PARAMETERS", f"'init.defaultBranch={DEFAULT_BRANCH}'"
            ):
                ds.create(cfg_proc=cfg_proc)
    finally:
        datalad.cfg.unset("datalad.repo.backend", scope="override")
    if backup_remote is not None:
        ds.repo.init_remote(
            backup_remote.name,
            [f"type={backup_remote.type}"]
            + [f"{k}={v}" for k, v in backup_remote.options.items()],
        )
        ds.repo.call_annex(["untrust", backup_remote.name])
        ds.repo.set_preferred_content(
            "wanted",
            "(not metadata=distribution-restrictions=*)",
            remote=backup_remote.name,
        )
    log.debug("Dataset for %s created", desc)


def create_github_sibling(
    ds: Dataset,
    owner: str,
    name: str,
    backup_remote: Optional[Remote],
    *,
    existing: str = "skip",
) -> bool:
    # Returns True iff sibling was created
    if "github" not in ds.repo.get_remotes():
        log.info("Creating GitHub sibling for %s", name)
        ds.create_sibling_github(
            reponame=name,
            existing=existing,
            name="github",
            access_protocol="https",
            github_organization=owner,
            publish_depends=backup_remote.name if backup_remote is not None else None,
        )
        ds.config.set(
            "remote.github.pushurl",
            f"git@github.com:{owner}/{name}.git",
            scope="local",
        )
        ds.config.set(f"branch.{DEFAULT_BRANCH}.remote", "github", scope="local")
        ds.config.set(
            f"branch.{DEFAULT_BRANCH}.merge",
            f"refs/heads/{DEFAULT_BRANCH}",
            scope="local",
        )
        return True
    else:
        log.debug("GitHub remote already exists for %s", name)
        return False


def maxdatetime(state: Optional[datetime], candidate: datetime) -> datetime:
    if state is None or state < candidate:
        return candidate
    else:
        return state


def is_meta_file(path: str, dandiset: bool = False) -> bool:
    root = path.split("/")[0]
    if dandiset and root == dandiset_metadata_file:
        return True
    return root in (".dandi", ".datalad", ".git", ".gitattributes", ".gitmodules")
