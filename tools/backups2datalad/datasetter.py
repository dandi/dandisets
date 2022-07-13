from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
import json
import logging
from operator import attrgetter
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import AsyncGenerator, Optional, Sequence

import anyio
from anyio.abc import AsyncResource
from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import RemoteZarrAsset
from dandi.exceptions import NotFoundError
from datalad.api import clone
from ghrepo import GHRepo
from humanize import naturalsize
from packaging.version import Version as PkgVersion

from .adandi import AsyncDandiClient, RemoteDandiset
from .adataset import AsyncDataset, DatasetStats
from .aioutil import aruncmd, pool_amap
from .config import BackupConfig
from .consts import DEFAULT_BRANCH
from .logging import log, quiet_filter
from .manager import GitHub, Manager
from .syncer import Syncer
from .util import AssetTracker, assets_eq, quantify, update_dandiset_metadata
from .zarr import ZarrLink, sync_zarr

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class DandiDatasetter(AsyncResource):
    dandi_client: AsyncDandiClient
    manager: Manager = field(init=False)
    config: BackupConfig

    def __post_init__(self) -> None:
        if self.config.gh_org is not None:
            token = subprocess.run(
                ["git", "config", "hub.oauthtoken"],
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            ).stdout.strip()
            gh = GitHub(token)
        else:
            gh = None
        self.manager = Manager(config=self.config, gh=gh, log=log)

    async def aclose(self) -> None:
        await self.dandi_client.aclose()
        await self.manager.aclose()

    async def ensure_superdataset(self) -> AsyncDataset:
        superds = AsyncDataset(self.config.dandiset_root)
        await superds.ensure_installed("superdataset")
        return superds

    async def update_from_backup(
        self,
        dandiset_ids: Sequence[str] = (),
        exclude: Optional[re.Pattern[str]] = None,
    ) -> None:
        superds = await self.ensure_superdataset()
        report = await pool_amap(
            self.update_dandiset,
            self.get_dandisets(dandiset_ids, exclude=exclude),
            workers=self.config.workers,
        )
        to_save: list[str] = []
        ds_stats: list[DatasetStats] = []
        for d, stats in report.results:
            to_save.append(d.identifier)
            if self.config.gh_org:
                ds_stats.append(stats)
        if to_save:
            log.debug("Committing superdataset")
            for did in to_save:
                await superds.ensure_subdataset(AsyncDataset(superds.pathobj / did))
            await superds.save(message="CRON update", path=to_save + [".gitmodules"])
            log.debug("Superdataset committed")
        if report.failed:
            raise RuntimeError(
                f"Backups for {quantify(len(report.failed), 'Dandiset')} failed"
            )
        elif self.config.gh_org is not None and not dandiset_ids and exclude is None:
            await self.set_superds_description(superds, ds_stats)

    async def init_dataset(
        self, dsdir: Path, dandiset_id: str, create_time: datetime
    ) -> AsyncDataset:
        ds = AsyncDataset(dsdir)
        await ds.ensure_installed(
            desc=f"Dandiset {dandiset_id}",
            commit_date=create_time,
            backup_remote=self.config.dandisets.remote,
        )
        return ds

    async def ensure_github_remote(self, ds: AsyncDataset, dandiset_id: str) -> None:
        if self.config.gh_org is not None:
            if await ds.create_github_sibling(
                owner=self.config.gh_org,
                name=dandiset_id,
                backup_remote=self.config.dandisets.remote,
            ):
                await self.manager.edit_github_repo(
                    GHRepo(self.config.gh_org, dandiset_id),
                    homepage=f"https://identifiers.org/DANDI:{dandiset_id}",
                )

    async def update_dandiset(
        self, dandiset: RemoteDandiset, ds: Optional[AsyncDataset] = None
    ) -> DatasetStats:
        if ds is None:
            ds = await self.init_dataset(
                self.config.dandiset_root / dandiset.identifier,
                dandiset_id=dandiset.identifier,
                create_time=dandiset.version.created,
            )
        dmanager = self.manager.with_sublogger(f"Dandiset {dandiset.identifier}")
        changed, zarr_stats = await self.sync_dataset(dandiset, ds, dmanager)
        await self.ensure_github_remote(ds, dandiset.identifier)
        await self.tag_releases(dandiset, ds, push=self.config.gh_org is not None)
        stats, _ = await ds.get_stats(cache=zarr_stats)
        if self.config.gh_org is not None:
            if changed:
                dmanager.log.info("Pushing to sibling")
                await ds.push(to="github", jobs=self.config.jobs, data="nothing")
            await self.manager.set_dandiset_description(dandiset, stats)
        return stats

    async def sync_dataset(
        self, dandiset: RemoteDandiset, ds: AsyncDataset, manager: Manager
    ) -> tuple[bool, dict[str, DatasetStats]]:
        # Returns:
        # - true iff any changes were committed to the repository
        # - a mapping from Zarr IDs to their dataset stats
        manager.log.info("Syncing")
        if await ds.is_dirty():
            raise RuntimeError(f"Dirty {dandiset}; clean or save before running")
        tracker = await anyio.to_thread.run_sync(AssetTracker.from_dataset, ds.pathobj)
        syncer = Syncer(manager=manager, dandiset=dandiset, ds=ds, tracker=tracker)
        await update_dandiset_metadata(dandiset, ds)
        await syncer.sync_assets()
        await syncer.prune_deleted()
        syncer.dump_asset_metadata()
        assert syncer.report is not None
        manager.log.debug("Checking whether repository is dirty ...")
        if await ds.is_unclean():
            manager.log.info("Committing changes")
            await ds.save(
                message=syncer.get_commit_message(),
                commit_date=dandiset.version.modified,
            )
            manager.log.debug("Commit made")
            syncer.report.commits += 1
        else:
            manager.log.debug("Repository is clean")
            if syncer.report.commits == 0:
                manager.log.info("No changes made to repository")
        manager.log.debug("Running `git gc`")
        await ds.gc()
        manager.log.debug("Finished running `git gc`")
        assert syncer.report.zarr_stats is not None
        return (syncer.report.commits > 0, syncer.report.zarr_stats)

    async def update_github_metadata(
        self,
        dandiset_ids: Sequence[str],
        exclude: Optional[re.Pattern[str]],
    ) -> None:
        ds_stats: list[DatasetStats] = []
        async for d in self.get_dandisets(dandiset_ids, exclude=exclude):
            ds = AsyncDataset(self.config.dandiset_root / d.identifier)
            stats, zarrstats = await ds.get_stats()
            await self.manager.set_dandiset_description(d, stats)
            for zarr_id, zarr_stat in zarrstats.items():
                await self.manager.set_zarr_description(zarr_id, zarr_stat)
            ds_stats.append(stats)
        if not dandiset_ids and exclude is None:
            superds = AsyncDataset(self.config.dandiset_root)
            await self.set_superds_description(superds, ds_stats)

    async def get_dandisets(
        self, dandiset_ids: Sequence[str], exclude: Optional[re.Pattern[str]]
    ) -> AsyncGenerator[RemoteDandiset, None]:
        if dandiset_ids:
            diter = self.dandi_client.get_dandisets_by_ids(dandiset_ids)
        else:
            diter = self.dandi_client.get_dandisets()
        async with aclosing(diter):
            async for d in diter:
                if exclude is not None and exclude.search(d.identifier):
                    log.debug("Skipping dandiset %s", d.identifier)
                else:
                    yield d

    async def set_superds_description(
        self, superds: AsyncDataset, ds_stats: list[DatasetStats]
    ) -> None:
        log.info("Setting repository description for superdataset")
        repo = await superds.get_ghrepo()
        total_size = naturalsize(sum(s.size for s in ds_stats))
        desc = (
            f"{quantify(len(ds_stats), 'Dandiset')}, {total_size} total."
            "  DataLad super-dataset of all Dandisets from https://github.com/dandisets"
        )
        await self.manager.edit_github_repo(repo, description=desc)

    async def tag_releases(
        self, dandiset: RemoteDandiset, ds: AsyncDataset, push: bool
    ) -> None:
        if not self.config.enable_tags:
            return
        log.info("Tagging releases for Dandiset %s", dandiset.identifier)
        versions = [v async for v in dandiset.aget_versions(include_draft=False)]
        for v in versions:
            if await ds.read_git("tag", "-l", v.identifier):
                log.debug("Version %s already tagged", v.identifier)
            else:
                log.info("Tagging version %s", v.identifier)
                await self.mkrelease(dandiset.for_version(v), ds, push=push)
        if versions:
            latest = max(map(attrgetter("identifier"), versions), key=PkgVersion)
            description = await ds.read_git("describe", "--tags", "--long", "--always")
            if "-" not in description:
                # No tags on default branch
                merge = True
            else:
                m = re.fullmatch(
                    r"(?P<tag>.+)-(?P<distance>[0-9]+)-g(?P<rev>[0-9a-f]+)?",
                    description,
                )
                assert m, f"Could not parse `git describe` output: {description!r}"
                merge = PkgVersion(latest) > PkgVersion(m["tag"])
            if merge:
                await ds.call_git(
                    "merge",
                    "-s",
                    "ours",
                    "-m",
                    f"Merge '{latest}' into drafts branch (no differences in"
                    " content merged)",
                    latest,
                )
            if push:
                await ds.push(to="github", jobs=self.config.jobs, data="nothing")

    async def mkrelease(
        self,
        dandiset: RemoteDandiset,
        ds: AsyncDataset,
        push: bool,
        commitish: Optional[str] = None,
    ) -> None:
        # `dandiset` must have its version set to the published version
        remote_assets = [asset async for asset in dandiset.aget_assets()]

        async def commit_has_assets(commit_hash: str) -> bool:
            repo_assets = json.loads(
                await ds.read_git("show", f"{commit_hash}:.dandi/assets.json")
            )
            return (not remote_assets and not repo_assets) or (
                repo_assets
                and isinstance(repo_assets[0], dict)
                and "asset_id" in repo_assets[0]
                and assets_eq(remote_assets, repo_assets)
            )

        candidates: list[str]
        if commitish is None:
            candidates = []
            # --before orders by commit date, not author date, so we need to
            # filter commits ourselves.
            commits = (
                await ds.read_git(
                    "log", r"--grep=\[backups2datalad\]", "--format=%H %aI"
                )
            ).splitlines()
            for cmt in commits:
                chash, _, cdate = cmt.partition(" ")
                ts = datetime.fromisoformat(cdate)
                if ts <= dandiset.version.created:
                    candidates.append(chash)
                    break
            assert candidates, "we should have had at least a single commit"
            if (
                # --reverse is applied after -n 1, so we cannot use it to get
                # just one commit in chronological order after the first
                # candidate, so we will get all and take last
                cmt := await ds.read_git(
                    "rev-list",
                    r"--grep=\[backups2datalad\]",
                    f"{candidates[0]}..HEAD",
                )
            ) != "":
                candidates.append(cmt.split()[-1])
        else:
            candidates = [commitish]
        matching = [c for c in candidates if await commit_has_assets(c)]
        assert len(matching) < 2, (
            f"Commits both before and after {dandiset.version.created} have"
            " matching asset metadata"
        )
        if matching:
            log.info(
                "Found commit %s with matching asset metadata;"
                " updating Dandiset metadata",
                matching[0],
            )
            await ds.call_git(
                "checkout", "-b", f"release-{dandiset.version_id}", matching[0]
            )
            await update_dandiset_metadata(dandiset, ds)
            log.debug("Committing changes")
            await ds.save(
                message=f"[backups2datalad] {dandiset_metadata_file} updated",
                commit_date=dandiset.version.created,
            )
            log.debug("Commit made")
        else:
            log.info(
                "Assets in candidate commits do not match assets in version %s;"
                " syncing",
                dandiset.version_id,
            )
            await ds.call_git(
                "checkout", "-b", f"release-{dandiset.version_id}", candidates[0]
            )
            await self.sync_dataset(
                dandiset,
                ds,
                self.manager.with_sublogger(f"Dandiset {dandiset.identifier}"),
            )
        await ds.call_git(
            "tag",
            "-m",
            f"Version {dandiset.version_id} of Dandiset {dandiset.identifier}",
            dandiset.version_id,
            env={
                **os.environ,
                "GIT_COMMITTER_NAME": "DANDI User",
                "GIT_COMMITTER_EMAIL": "info@dandiarchive.org",
                "GIT_COMMITTER_DATE": str(dandiset.version.created),
            },
        )
        await ds.call_git("checkout", DEFAULT_BRANCH)
        await ds.call_git("branch", "-D", f"release-{dandiset.version_id}")
        if push:
            await ds.call_git("push", "github", dandiset.version_id)

    async def backup_zarrs(self, dandiset: str, partial_dir: Path) -> None:
        assert self.config.zarr_root is not None
        partial_dir.mkdir(parents=True, exist_ok=True)
        d = await self.dandi_client.get_dandiset(dandiset, "draft")
        ds = AsyncDataset(self.config.dandiset_root / d.identifier)
        dslock = anyio.Lock()

        async def dobackup(asset: RemoteZarrAsset) -> None:
            try:
                zarr_digest = asset.get_digest().value
            except NotFoundError:
                log.info(
                    "%s: Zarr checksum has not been computed yet; not backing up",
                    asset.path,
                )
                return
            ultimate_dspath = self.config.zarr_root / asset.zarr
            if ultimate_dspath.exists():
                log.info("%s: Zarr already backed up", asset.path)
                return
            zarr_dspath = partial_dir / asset.zarr
            zl = ZarrLink(
                zarr_dspath=ultimate_dspath,
                timestamp=None,
                asset_paths=[asset.path],
            )
            await sync_zarr(
                asset,
                zarr_digest,
                zarr_dspath,
                self.manager.with_sublogger(f"Zarr {asset.zarr}"),
                link=zl,
            )
            log.info("Zarr %s: Moving dataset", asset.zarr)
            zarr_dspath.rename(ultimate_dspath)
            ts = zl.timestamp
            if ts is None:
                # Zarr was already up to date; get timestamp from its latest
                # commit
                zds = AsyncDataset(ultimate_dspath)
                ts = datetime.fromisoformat(
                    await zds.read_git("show", "-s", "--format=%aI", "HEAD")
                )
            assert not (ds.pathobj / asset.path).exists()
            log.debug("Waiting for lock on Dandiset dataset")
            async with dslock:
                log.info("Zarr %s: cloning to %s", asset.zarr, asset.path)
                if self.config.zarr_gh_org is not None:
                    src = f"https://github.com/{self.config.zarr_gh_org}/{asset.zarr}"
                else:
                    src = str(ultimate_dspath)
                await anyio.to_thread.run_sync(
                    partial(clone, source=src, path=ds.pathobj / asset.path)
                )
                if self.config.zarr_gh_org is not None:
                    await aruncmd(
                        "git",
                        "remote",
                        "rename",
                        "origin",
                        "github",
                        cwd=ds.pathobj / asset.path,
                    )
                log.debug("Zarr %s: Finished cloning", asset.zarr)
                await ds.ensure_subdataset(AsyncDataset(ds.pathobj / asset.path))
                log.debug("Zarr %s: Saving changes to Dandiset dataset", asset.zarr)
                await ds.save(
                    f"[backups2datalad] Backed up Zarr {asset.zarr} to {asset.path}",
                    path=[asset.path, ".gitmodules"],
                    commit_date=ts,
                )
                log.debug("Zarr %s: Changes saved", asset.zarr)

        report = await pool_amap(
            dobackup, d.aget_zarr_assets(), workers=self.config.workers
        )
        if report.failed:
            raise RuntimeError(f"{quantify(len(report.failed), 'Zarr backup')} failed")

    async def debug_logfile(self, quiet_debug: bool) -> None:
        """
        Log all log messages at DEBUG or higher to a file without disrupting or
        altering the logging to the screen
        """
        root = logging.getLogger()
        screen_level = root.getEffectiveLevel()
        root.setLevel(logging.NOTSET)
        for h in root.handlers:
            if quiet_debug:
                h.addFilter(quiet_filter)
            else:
                h.setLevel(screen_level)
        # Superdataset must exist before creating anything in the directory:
        await self.ensure_superdataset()
        logdir = self.config.dandiset_root / ".git" / "dandi" / "backups2datalad"
        logdir.mkdir(exist_ok=True, parents=True)
        filename = "{:%Y.%m.%d.%H.%M.%SZ}.log".format(datetime.utcnow())
        logfile = logdir / filename
        handler = logging.FileHandler(logfile, encoding="utf-8")
        handler.setLevel(logging.DEBUG)
        fmter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        handler.setFormatter(fmter)
        root.addHandler(handler)
        log.info("Saving logs to %s", logfile)
