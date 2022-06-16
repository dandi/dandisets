from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import logging
from operator import attrgetter
import os
from pathlib import Path
import re
import sys
from typing import Any, AsyncGenerator, Optional, Sequence

import anyio
from anyio.abc import AsyncResource
from dandi.consts import dandiset_metadata_file
from ghrepo import GHRepo
import httpx
from humanize import naturalsize
from packaging.version import Version as PkgVersion

from .adandi import AsyncDandiClient, RemoteDandiset
from .adataset import AsyncDataset, ObjectType
from .aioutil import areadcmd, arequest, pool_amap
from .config import BackupConfig
from .consts import DEFAULT_BRANCH, USER_AGENT
from .logging import PrefixedLogger, log
from .syncer import Syncer
from .util import (
    AssetTracker,
    assets_eq,
    is_meta_file,
    quantify,
    update_dandiset_metadata,
)

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class DandiDatasetter(AsyncResource):
    dandi_client: AsyncDandiClient
    config: BackupConfig
    gh: Optional[httpx.AsyncClient] = None

    async def aclose(self) -> None:
        await self.dandi_client.aclose()
        if self.gh is not None:
            await self.gh.aclose()

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
        ds_stats: list[DandisetStats] = []
        for d, stats in report.results:
            to_save.append(d)
            if self.config.gh_org:
                assert stats is not None
                ds_stats.append(stats)
        log.debug("Committing superdataset")
        await superds.save(message="CRON update", path=to_save)
        log.debug("Superdataset committed")
        if self.config.gh_org is not None and not dandiset_ids and exclude is None:
            await self.set_superds_description(superds, ds_stats)
        if report.failed:
            raise RuntimeError(
                f"Backups for {quantify(len(report.failed), 'Dandiset')} failed"
            )

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
                await self.edit_github_repo(
                    GHRepo(self.config.gh_org, dandiset_id),
                    homepage=f"https://identifiers.org/DANDI:{dandiset_id}",
                )

    async def update_dandiset(
        self, dandiset: RemoteDandiset, ds: Optional[AsyncDataset] = None
    ) -> Optional[DandisetStats]:
        if ds is None:
            ds = await self.init_dataset(
                self.config.dandiset_root / dandiset.identifier,
                dandiset_id=dandiset.identifier,
                create_time=dandiset.version.created,
            )
        dlog = log.sublogger(f"Dandiset {dandiset.identifier}")
        changed = await self.sync_dataset(dandiset, ds, dlog)
        await self.ensure_github_remote(ds, dandiset.identifier)
        await self.tag_releases(dandiset, ds, push=self.config.gh_org is not None)
        if self.config.gh_org is not None:
            if changed:
                dlog.info("Pushing to sibling")
                await ds.push(to="github", jobs=self.config.jobs, data="nothing")
            return await self.set_dandiset_gh_metadata(dandiset, ds)
        else:
            return None

    async def sync_dataset(
        self, dandiset: RemoteDandiset, ds: AsyncDataset, dlog: PrefixedLogger
    ) -> bool:
        # Returns true if any changes were committed to the repository
        dlog.info("Syncing")
        if await ds.is_dirty():
            raise RuntimeError(f"Dirty {dandiset}; clean or save before running")
        tracker = await anyio.to_thread.run_sync(AssetTracker.from_dataset, ds.pathobj)
        syncer = Syncer(
            config=self.config, dandiset=dandiset, ds=ds, tracker=tracker, log=dlog
        )
        await update_dandiset_metadata(dandiset, ds)
        await syncer.sync_assets()
        await syncer.prune_deleted()
        syncer.dump_asset_metadata()
        assert syncer.report is not None
        dlog.debug("Checking whether repository is dirty ...")
        if await ds.is_unclean():
            dlog.info("Committing changes")
            await ds.save(
                message=syncer.get_commit_message(),
                commit_date=dandiset.version.modified,
            )
            dlog.debug("Commit made")
            syncer.report.commits += 1
        else:
            dlog.debug("Repository is clean")
            if syncer.report.commits == 0:
                dlog.info("No changes made to repository")
        dlog.debug("Running `git gc`")
        await ds.gc()
        dlog.debug("Finished running `git gc`")
        return syncer.report.commits > 0

    async def get_remote_url(self, ds: AsyncDataset) -> str:
        upstream = await ds.get_repo_config(f"branch.{DEFAULT_BRANCH}.remote")
        if upstream is None:
            raise ValueError(
                f"Upstream branch not set for {DEFAULT_BRANCH} in {ds.path}"
            )
        url = await ds.get_repo_config(f"remote.{upstream}.url")
        if url is None:
            raise ValueError(f"{upstream!r} remote URL not set for {ds.path}")
        return url

    async def get_ghrepo_for_dataset(self, ds: AsyncDataset) -> GHRepo:
        url = await self.get_remote_url(ds)
        return GHRepo.parse_url(url)

    async def update_github_metadata(
        self,
        dandiset_ids: Sequence[str],
        exclude: Optional[re.Pattern[str]],
    ) -> None:
        ds_stats: list[DandisetStats] = []
        async for d in self.get_dandisets(dandiset_ids, exclude=exclude):
            ds = AsyncDataset(self.config.dandiset_root / d.identifier)
            ds_stats.append(await self.set_dandiset_gh_metadata(d, ds))
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

    async def get_dandiset_stats(
        self, ds: AsyncDataset
    ) -> tuple[DandisetStats, dict[str, DandisetStats]]:
        files = 0
        size = 0
        substats: dict[str, DandisetStats] = {}
        for filestat in await ds.get_file_stats():
            path = Path(filestat.path)
            if not is_meta_file(path.parts[0], dandiset=True):
                if filestat.type is ObjectType.COMMIT:
                    zarr_ds = AsyncDataset(ds.pathobj / path)
                    zarr_id = Path(await self.get_remote_url(zarr_ds)).name
                    try:
                        zarr_stat = substats[zarr_id]
                    except KeyError:
                        zarr_stat, subsubstats = await self.get_dandiset_stats(zarr_ds)
                        assert not subsubstats
                        substats[zarr_id] = zarr_stat
                    files += zarr_stat.files
                    size += zarr_stat.size
                else:
                    files += 1
                    assert filestat.size is not None
                    size += filestat.size
        return (DandisetStats(files=files, size=size), substats)

    async def set_dandiset_gh_metadata(
        self, d: RemoteDandiset, ds: AsyncDataset
    ) -> DandisetStats:
        assert self.config.gh_org is not None
        assert self.config.zarr_gh_org is not None
        stats, zarrstats = await self.get_dandiset_stats(ds)
        await self.edit_github_repo(
            GHRepo(self.config.gh_org, d.identifier),
            homepage=f"https://identifiers.org/DANDI:{d.identifier}",
            description=await self.describe_dandiset(d, stats),
        )
        for zarr_id, zarr_stat in zarrstats.items():
            await self.edit_github_repo(
                GHRepo(self.config.zarr_gh_org, zarr_id),
                description=self.describe_zarr(zarr_stat),
            )
        return stats

    async def describe_dandiset(
        self, dandiset: RemoteDandiset, stats: DandisetStats
    ) -> str:
        metadata = await dandiset.aget_raw_metadata()
        desc = dandiset.version.name
        contact = ", ".join(
            c["name"]
            for c in metadata.get("contributor", [])
            if "dandi:ContactPerson" in c.get("roleName", []) and "name" in c
        )
        if contact:
            desc = f"{contact}, {desc}"
        versions = 0
        async for v in dandiset.aget_versions(include_draft=False):
            versions += 1
        if versions:
            desc = f"{quantify(versions, 'release')}, {desc}"
        size = naturalsize(stats.size)
        return f"{quantify(stats.files, 'file')}, {size}, {desc}"

    def describe_zarr(self, stats: DandisetStats) -> str:
        size = naturalsize(stats.size)
        return f"{quantify(stats.files, 'file')}, {size}"

    async def set_superds_description(
        self, superds: AsyncDataset, ds_stats: list[DandisetStats]
    ) -> None:
        log.info("Setting repository description for superdataset")
        repo = await self.get_ghrepo_for_dataset(superds)
        total_size = naturalsize(sum(s.size for s in ds_stats))
        desc = (
            f"{quantify(len(ds_stats), 'Dandiset')}, {total_size} total."
            "  DataLad super-dataset of all Dandisets from https://github.com/dandisets"
        )
        await self.edit_github_repo(repo, description=desc)

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
                dandiset, ds, log.sublogger(f"Dandiset {dandiset.identifier}")
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

    async def edit_github_repo(self, repo: GHRepo, **kwargs: Any) -> None:
        if self.gh is None:
            token = await areadcmd("git", "config", "hub.oauthtoken")
            self.gh = httpx.AsyncClient(
                headers={"Authorization": f"token {token}", "User-Agent": USER_AGENT},
                follow_redirects=True,
            )
        log.debug("Editing repository %s", repo)
        # Retry on 404's in case we're calling this right after
        # create_github_sibling(), when the repo may not yet exist
        await arequest(self.gh, "PATCH", repo.api_url, json=kwargs, retry_on=[404])

    async def debug_logfile(self) -> None:
        """
        Log all log messages at DEBUG or higher to a file without disrupting or
        altering the logging to the screen
        """
        root = logging.getLogger()
        screen_level = root.getEffectiveLevel()
        root.setLevel(logging.NOTSET)
        for h in root.handlers:
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


@dataclass
class DandisetStats:
    files: int
    size: int
