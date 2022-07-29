from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import textwrap
from types import TracebackType
from typing import Any, Iterator, Optional, Type

from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import RemoteAsset, RemoteDandiset
from dandi.dandiset import APIDandiset
from datalad.api import Dataset
from datalad.support.json_py import dump

from .config import BackupConfig
from .logging import PrefixedLogger


@dataclass
class AssetTracker:
    #: The path to the .dandi/assets.json file that this instance manages
    filepath: Path
    #: Paths of files found when the syncing started, minus the paths for any
    #: assets downloaded during syncing
    local_assets: set[str]
    #: Metadata for assets currently being downloaded, as a mapping from asset
    #: paths to metadata
    in_progress: dict[str, dict] = field(init=False, default_factory=dict)
    #: Asset metadata from previous sync, plus metadata for any assets
    #: completely downloaded during the sync, as a mapping from asset paths to
    #: metadata
    asset_metadata: dict[str, dict]
    #: Paths of assets that are not being downloaded this run due to a lack of
    #: SHA256 digests
    future_assets: set[str] = field(init=False, default_factory=set)

    @classmethod
    def from_dataset(cls, dspath: Path) -> AssetTracker:
        filepath = dspath / ".dandi" / "assets.json"
        local_assets = set(dataset_files(dspath))
        asset_metadata: dict[str, dict] = {}
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

    def get_deleted(self, config: BackupConfig) -> Iterator[str]:
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


def custom_commit_env(dt: Optional[datetime]) -> dict[str, str]:
    env = os.environ.copy()
    if dt is not None:
        env["GIT_AUTHOR_NAME"] = "DANDI User"
        env["GIT_AUTHOR_EMAIL"] = "info@dandiarchive.org"
        env["GIT_AUTHOR_DATE"] = str(dt)
    return env


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


def asset2dict(asset: RemoteAsset) -> dict[str, Any]:
    return {**asset.json_dict(), "metadata": asset.get_raw_metadata()}


def assets_eq(remote_assets: list[RemoteAsset], local_assets: list[dict]) -> bool:
    return {a.identifier: asset2dict(a) for a in remote_assets} == {
        a["asset_id"]: a for a in local_assets
    }


async def update_dandiset_metadata(
    dandiset: RemoteDandiset, ds: Dataset, log: PrefixedLogger
) -> None:
    log.info("Updating metadata file")
    (ds.pathobj / dandiset_metadata_file).unlink(missing_ok=True)
    metadata = await dandiset.aget_raw_metadata()
    APIDandiset(ds.pathobj, allow_empty=True).update_metadata(metadata)
    await ds.add(dandiset_metadata_file)


def quantify(qty: int, singular: str, plural: Optional[str] = None) -> str:
    if qty == 1:
        return f"{qty} {singular}"
    elif plural is None:
        return f"{qty} {singular}s"
    else:
        return f"{qty} {plural}"


def key2hash(key: str) -> str:
    return key.split("-")[-1].partition(".")[0]


def format_errors(messages: list[str]) -> str:
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
