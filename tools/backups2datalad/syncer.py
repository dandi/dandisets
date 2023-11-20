from __future__ import annotations

from dataclasses import dataclass, field

from .adandi import RemoteDandiset
from .adataset import AsyncDataset
from .asyncer import Report, async_assets
from .config import BackupConfig
from .logging import PrefixedLogger
from .manager import Manager
from .util import AssetTracker, UnexpectedChangeError, quantify


@dataclass
class Syncer:
    manager: Manager
    dandiset: RemoteDandiset
    ds: AsyncDataset
    tracker: AssetTracker
    deleted: int = 0
    # value of garbage_assets will be assigned but to pacify mypy - assign factory
    garbage_assets: list[str] = field(default_factory=list)
    report: Report | None = None

    @property
    def config(self) -> BackupConfig:
        return self.manager.config

    @property
    def log(self) -> PrefixedLogger:
        return self.manager.log

    async def sync_assets(self, error_on_change: bool = False) -> None:
        self.log.info("Syncing assets...")
        self.report = await async_assets(
            self.dandiset, self.ds, self.manager, self.tracker, error_on_change
        )
        self.log.info("Asset sync complete!")
        self.log.info("%s added", quantify(self.report.added, "asset"))
        self.log.info("%s updated", quantify(self.report.updated, "asset"))
        self.log.info("%s registered", quantify(self.report.registered, "asset"))
        self.log.info(
            "%s successfully downloaded", quantify(self.report.downloaded, "asset")
        )
        self.report.check()

    async def prune_deleted(self, error_on_change: bool = False) -> None:
        for asset_path in self.tracker.get_deleted(self.config):
            if error_on_change:
                raise UnexpectedChangeError(
                    f"Dandiset {self.dandiset.identifier}: Asset {asset_path!r}"
                    " deleted from Dandiset but timestamp was not updated on"
                    " server"
                )
            self.log.info(
                "%s: Asset is in dataset but not in Dandiarchive; deleting", asset_path
            )
            await self.ds.remove(asset_path)
            self.deleted += 1

    async def dump_asset_metadata(self) -> None:
        self.garbage_assets = self.tracker.prune_metadata()
        if self.garbage_assets and not self.config.gc_assets:
            # to ease troubleshooting, let's list some which were GCed
            listing = ", ".join(self.garbage_assets[:3])
            if len(self.garbage_assets) > 3:
                listing += f" and {len(self.garbage_assets) - 3} more."
            raise UnexpectedChangeError(
                f"Dandiset {self.dandiset.identifier}:"
                f" {quantify(len(self.garbage_assets), 'asset')}"
                f" garbage-collected from assets.json: {listing}"
            )
        self.tracker.dump()
        await self.ds.add(".dandi/assets.json")

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
                f"{quantify(len(self.garbage_assets), 'asset')} garbage-collected"
                " from .dandi/assets.json"
            )
        if futures := self.tracker.future_qty:
            msgparts.append(f"{quantify(futures, 'asset')} not yet downloaded")
        if not msgparts:
            msgparts.append("Only some metadata updates")
        return f"[backups2datalad] {', '.join(msgparts)}"
