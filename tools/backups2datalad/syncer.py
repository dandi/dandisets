from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from dandi.dandiapi import RemoteDandiset
from datalad.api import Dataset
import trio

from . import log
from .asyncer import async_assets
from .util import AssetTracker, Config, Report, quantify


@dataclass
class Syncer:
    config: Config
    dandiset: RemoteDandiset
    ds: Dataset
    deleted: int = 0
    report: Optional[Report] = None
    tracker: AssetTracker = field(init=False)

    def __post_init__(self) -> None:
        self.tracker = AssetTracker.from_dataset(self.ds.pathobj)

    def __enter__(self) -> Syncer:
        return self

    def __exit__(self, *_exc_info: Any) -> None:
        # This used to do something, but now I'm mainly just keeping it around
        # because the `with` block makes the code look nicer.
        pass

    def sync_assets(self) -> None:
        log.info("Syncing assets...")
        report = trio.run(
            async_assets, self.dandiset, self.ds.pathobj, self.config, self.tracker
        )
        log.info("Asset sync complete!")
        log.info("%s added", quantify(report.added, "asset"))
        log.info("%s updated", quantify(report.updated, "asset"))
        log.info("%s sucessfully downloaded", quantify(report.downloaded, "asset"))
        report.check()

    def prune_deleted(self) -> None:
        for asset_path in self.tracker.get_deleted(self.config):
            log.info(
                "%s: Asset is in dataset but not in Dandiarchive; deleting", asset_path
            )
            self.ds.repo.remove([asset_path])
            self.deleted += 1

    def dump_asset_metadata(self) -> None:
        self.tracker.dump(self.ds.pathobj)

    def get_commit_message(self) -> str:
        assert self.report is not None
        msgparts = []
        if self.report.added:
            msgparts.append(f"{quantify(self.report.added, 'file')} added")
        if self.report.updated:
            msgparts.append(f"{quantify(self.report.updated, 'file')} updated")
        if self.deleted:
            msgparts.append(f"{quantify(self.deleted, 'file')} deleted")
        if futures := self.tracker.future_qty:
            msgparts.append(f"{quantify(futures, 'file')} not yet downloaded")
        if not msgparts:
            msgparts.append("Only some metadata updates")
        return f"[backups2datalad] {', '.join(msgparts)}"
