from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dandi.dandiapi import RemoteDandiset

from .adataset import AsyncDataset
from .asyncer import Report, async_assets
from .config import BackupConfig
from .logging import PrefixedLogger
from .manager import Manager
from .util import AssetTracker, quantify


@dataclass
class Syncer:
    manager: Manager
    dandiset: RemoteDandiset
    ds: AsyncDataset
    tracker: AssetTracker
    deleted: int = 0
    garbage_assets: int = 0
    report: Optional[Report] = None

    @property
    def config(self) -> BackupConfig:
        return self.manager.config

    @property
    def log(self) -> PrefixedLogger:
        return self.manager.log

    async def sync_assets(self) -> None:
        self.log.info("Syncing assets...")
        self.report = await async_assets(
            self.dandiset, self.ds, self.manager, self.tracker
        )
        self.log.info("Asset sync complete!")
        self.log.info("%s added", quantify(self.report.added, "asset"))
        self.log.info("%s updated", quantify(self.report.updated, "asset"))
        self.log.info("%s registered", quantify(self.report.registered, "asset"))
        self.log.info(
            "%s successfully downloaded", quantify(self.report.downloaded, "asset")
        )
        self.report.check()

    async def prune_deleted(self) -> None:
        for asset_path in self.tracker.get_deleted(self.config):
            self.log.info(
                "%s: Asset is in dataset but not in Dandiarchive; deleting", asset_path
            )
            await self.ds.remove(asset_path)
            self.deleted += 1

    def dump_asset_metadata(self) -> None:
        self.garbage_assets = self.tracker.prune_metadata()
        self.tracker.dump()

    def get_commit_message(self) -> str:
        msgparts = []
        if self.dandiset.version_id != "draft":
            assert self.report is not None
            if self.report.added:
                msgparts.append(f"{quantify(self.report.added, 'file')} added")
            if self.report.updated:
                msgparts.append(f"{quantify(self.report.updated, 'file')} updated")
        if self.deleted:
            msgparts.append(f"{quantify(self.deleted, 'file')} deleted")
        if self.garbage_assets:
            msgparts.append(
                f"{quantify(self.garbage_assets, 'asset')} garbage-collected"
                " from .dandi/assets.json"
            )
        if futures := self.tracker.future_qty:
            msgparts.append(f"{quantify(futures, 'asset')} not yet downloaded")
        if not msgparts:
            msgparts.append("Only some metadata updates")
        return f"[backups2datalad] {', '.join(msgparts)}"
