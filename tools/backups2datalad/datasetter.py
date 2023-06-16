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
import shutil
from socket import gethostname
import subprocess
import sys
from typing import AsyncGenerator, Optional, Sequence

import anyio
from anyio.abc import AsyncResource
from dandi.consts import dandiset_metadata_file
from dandi.exceptions import NotFoundError
from datalad.api import clone
from ghrepo import GHRepo
from humanize import naturalsize
from linesep import split_terminated
from packaging.version import Version as PkgVersion

from .adandi import AsyncDandiClient, RemoteDandiset, RemoteZarrAsset
from .adataset import AssetsState, AsyncDataset
from .aioutil import aruncmd, pool_amap
from .config import BackupConfig, Mode
from .consts import DEFAULT_BRANCH, GIT_OPTIONS
from .logging import PrefixedLogger, log, quiet_filter
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
    logfile: Optional[Path] = None

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
        for d, changed in report.results:
            if changed:
                to_save.append(d.identifier)
        if to_save:
            log.debug("Committing superdataset")
            superds.assert_no_duplicates_in_gitmodules()
            msg = await self.get_superds_commit_message(superds)
            await superds.save(message=msg, path=to_save)
            superds.assert_no_duplicates_in_gitmodules()
            log.debug("Superdataset committed")
        if report.failed:
            raise RuntimeError(
                f"Backups for {quantify(len(report.failed), 'Dandiset')} failed"
            )
        elif self.config.gh_org is not None:
            await self.set_superds_description(superds)

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
    ) -> bool:
        # Returns true iff any changes were committed to the repository
        if ds is None:
            ds = await self.init_dataset(
                self.config.dandiset_root / dandiset.identifier,
                dandiset_id=dandiset.identifier,
                create_time=dandiset.version.created,
            )
        dmanager = self.manager.with_sublogger(f"Dandiset {dandiset.identifier}")
        state = ds.get_assets_state()
        if (
            dmanager.config.mode is Mode.FORCE
            or state is None
            or state.timestamp < dandiset.version.modified
        ):
            changed = await self.sync_dataset(dandiset, ds, dmanager)
        elif state.timestamp > dandiset.version.modified:
            raise RuntimeError(
                f"Remote Dandiset {dandiset.identifier} has 'modified'"
                f" timestamp {dandiset.version.modified} BEFORE last-recorded"
                f" {state.timestamp}"
            )
        elif dmanager.config.mode is Mode.VERIFY:
            changed = await self.sync_dataset(
                dandiset, ds, dmanager, error_on_change=True
            )
        else:
            dmanager.log.info(
                "Remote Dandiset has not been modified since last backup; not syncing"
            )
            changed = False
        await self.ensure_github_remote(ds, dandiset.identifier)
        await self.tag_releases(
            dandiset, ds, push=self.config.gh_org is not None, log=dmanager.log
        )
        # Call `get_stats()` even if gh_org is None so that out-of-date stats
        # get updated:
        stats = await ds.get_stats(config=dmanager.config)
        if self.config.gh_org is not None:
            if changed:
                dmanager.log.info("Pushing to sibling")
                await ds.push(to="github", jobs=self.config.jobs, data="nothing")
            await self.manager.set_dandiset_description(dandiset, stats, ds)
        return changed

    async def sync_dataset(
        self,
        dandiset: RemoteDandiset,
        ds: AsyncDataset,
        manager: Manager,
        error_on_change: bool = False,
    ) -> bool:
        # Returns:
        # - true iff any changes were committed to the repository
        manager.log.info("Syncing")
        if await ds.is_dirty():
            raise RuntimeError(f"Dirty {dandiset}; clean or save before running")
        tracker = await anyio.to_thread.run_sync(AssetTracker.from_dataset, ds.pathobj)
        syncer = Syncer(manager=manager, dandiset=dandiset, ds=ds, tracker=tracker)
        await update_dandiset_metadata(dandiset, ds, log=manager.log)
        await syncer.sync_assets(error_on_change)
        await syncer.prune_deleted(error_on_change)
        await syncer.dump_asset_metadata()
        assert syncer.report is not None
        manager.log.debug("Checking whether repository is dirty ...")
        if await ds.is_unclean():
            manager.log.info("Committing changes")
            await ds.commit(
                message=syncer.get_commit_message(),
                commit_date=dandiset.version.modified,
            )
            manager.log.debug("Commit made")
            syncer.report.commits += 1
        else:
            manager.log.debug("Repository is clean")
            if syncer.report.commits == 0:
                manager.log.info("No changes made to repository")
        manager.log.info("Uninstalling submodules")
        await ds.uninstall_subdatasets()
        manager.log.info("Finished uninstalling submodules")
        manager.log.debug("Running `git gc`")
        await ds.gc()
        manager.log.debug("Finished running `git gc`")
        return syncer.report.commits > 0

    async def update_github_metadata(
        self,
        dandiset_ids: Sequence[str],
        exclude: Optional[re.Pattern[str]],
    ) -> None:
        async for d in self.get_dandisets(dandiset_ids, exclude=exclude):
            ds = AsyncDataset(self.config.dandiset_root / d.identifier)
            stats = await ds.get_stats(config=self.config)
            await self.manager.set_dandiset_description(d, stats, ds)
            for sub_info in await ds.get_subdatasets():
                # ATM we have only .zarr-like subdatasets
                assert Path(sub_info["path"]).suffix in (".zarr", ".ngff")
                # here we will rely on get_stats to use cached during above
                # recursive through subdatasets ds.get_stats call
                await self.manager.set_zarr_description(
                    *(await AsyncDataset.get_zarr_sub_stats(sub_info, self.config))
                )
        if not dandiset_ids and exclude is None:
            superds = AsyncDataset(self.config.dandiset_root)
            await self.set_superds_description(superds)

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
                    log.debug("Skipping Dandiset %s", d.identifier)
                else:
                    yield d

    async def set_superds_description(self, superds: AsyncDataset) -> None:
        log.info("Setting repository description for superdataset")
        repo = await superds.get_ghrepo()
        qty = 0
        size = 0
        for s in await superds.get_subdatasets():
            qty += 1
            ds = AsyncDataset(s["path"])
            if (stats := await ds.get_stored_stats()) is not None:
                size += stats.size
        total_size = naturalsize(size)
        desc = (
            f"{quantify(qty, 'Dandiset')}, {total_size} total."
            "  DataLad super-dataset of all Dandisets from https://github.com/dandisets"
        )
        await self.manager.edit_github_repo(repo, description=desc)

    async def tag_releases(
        self,
        dandiset: RemoteDandiset,
        ds: AsyncDataset,
        push: bool,
        log: PrefixedLogger,
    ) -> None:
        if not self.config.enable_tags:
            return
        log.info("Tagging releases for Dandiset")
        versions = [v async for v in dandiset.aget_versions(include_draft=False)]
        changed = False
        for v in versions:
            if await ds.read_git("tag", "-l", v.identifier):
                log.debug("Version %s already tagged", v.identifier)
            else:
                log.info("Tagging version %s", v.identifier)
                await self.mkrelease(dandiset.for_version(v), ds, push=push, log=log)
                changed = True
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
            if push and (changed or merge):
                await ds.push(to="github", jobs=self.config.jobs, data="nothing")

    async def mkrelease(
        self,
        dandiset: RemoteDandiset,
        ds: AsyncDataset,
        push: bool,
        log: PrefixedLogger,
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
        if len(matching) > 1:
            log.warning(
                "While tagging version %s of Dandiset %s, found candidate"
                " commits both before and after %s with matching asset"
                " metadata: %s",
                dandiset.version_id,
                dandiset.identifier,
                dandiset.version.created,
                ", ".join(matching),
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
            await update_dandiset_metadata(dandiset, ds, log)
            await ds.set_assets_state(AssetsState(timestamp=dandiset.version.created))
            log.debug("Committing changes")
            await ds.commit(
                message=f"[backups2datalad] {dandiset_metadata_file} updated",
                commit_date=dandiset.version.created,
            )
            log.debug("Commit made")
            log.debug("Running `git gc`")
            await ds.gc()
            log.debug("Finished running `git gc`")
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
            if self.config.gh_org is not None:
                assert self.manager.gh is not None
                await self.manager.gh.create_release(
                    GHRepo(self.config.gh_org, dandiset.identifier), dandiset.version_id
                )

    async def backup_zarrs(self, dandiset: str, partial_dir: Path) -> None:
        zarr_root = self.config.zarr_root
        assert zarr_root is not None
        partial_dir.mkdir(parents=True, exist_ok=True)
        d = await self.dandi_client.get_dandiset(dandiset, "draft")
        ds = AsyncDataset(self.config.dandiset_root / d.identifier)
        dslock = anyio.Lock()

        async def dobackup(asset: RemoteZarrAsset) -> None:
            try:
                zarr_digest = asset.get_digest_value()
            except NotFoundError:
                log.info(
                    "%s: Zarr checksum has not been computed yet; not backing up",
                    asset.path,
                )
                return
            assert zarr_root is not None
            ultimate_dspath = zarr_root / asset.zarr
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
            shutil.move(str(zarr_dspath), str(ultimate_dspath))
            log.info("Zarr %s: Finished moving dataset", asset.zarr)
            zds = AsyncDataset(ultimate_dspath)
            await zds.call_annex(
                "describe", "here", f"{gethostname()}:{ultimate_dspath}"
            )
            ts = zl.timestamp
            if ts is None:
                # Zarr was already up to date; get timestamp from its latest
                # commit
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
                        *GIT_OPTIONS,
                        "remote",
                        "rename",
                        "origin",
                        "github",
                        cwd=ds.pathobj / asset.path,
                    )
                # Uncomment this if `ds.save()` below is ever changed to
                # `ds.commit()`:
                # await ds.add_submodule(
                #     path=asset.path, url=src, datalad_id=await zds.get_datalad_id()
                # )
                log.debug("Zarr %s: Finished cloning", asset.zarr)
                log.debug("Zarr %s: Saving changes to Dandiset dataset", asset.zarr)
                ds.assert_no_duplicates_in_gitmodules()
                await ds.save(
                    f"[backups2datalad] Backed up Zarr {asset.zarr} to {asset.path}",
                    path=[asset.path],
                    commit_date=ts,
                )
                ds.assert_no_duplicates_in_gitmodules()
                log.debug("Zarr %s: Changes saved", asset.zarr)
                # now that we have as a subdataset and know that it is all good,
                # uninstall that subdataset
                await anyio.to_thread.run_sync(
                    partial(
                        ds.ds.drop,
                        what="datasets",
                        recursive=True,
                        path=[asset.path],
                        reckless="kill",
                    )
                )
                log.debug("Zarr %s: uninstalled", asset.zarr)

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
                h.addFilter(quiet_filter(logging.INFO))
            else:
                h.setLevel(screen_level)
        # Superdataset must exist before creating anything in the directory:
        await self.ensure_superdataset()
        logdir = self.config.dandiset_root / ".git" / "dandi" / "backups2datalad"
        logdir.mkdir(exist_ok=True, parents=True)
        filename = "{:%Y.%m.%d.%H.%M.%SZ}.log".format(datetime.utcnow())
        self.logfile = logdir / filename
        handler = logging.FileHandler(self.logfile, encoding="utf-8")
        handler.setLevel(logging.DEBUG)
        handler.addFilter(quiet_filter(logging.WARNING))
        fmter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        handler.setFormatter(fmter)
        root.addHandler(handler)
        log.info("Saving logs to %s", self.logfile)

    async def get_superds_commit_message(self, superds: AsyncDataset) -> str:
        added = []
        modified = []
        other = False
        for entry in split_terminated(
            await superds.read_git(
                "status", "--porcelain", "--ignore-submodules=dirty", "-z", strip=False
            ),
            "\0",
        ):
            status = entry[:2]
            path = entry[3:].rstrip("/")
            if status[0] == "A" or status == "??":
                added.append(path)
            elif status[0] == "M" or status[1] == "M":
                modified.append(path)
            else:
                other = True
        added_datasets: dict[str, list[str]] = {}
        modified_datasets: dict[str, list[str]] = {}
        for path in added:
            if (superds.pathobj / path / ".git").exists():
                commits = (
                    await AsyncDataset(superds.pathobj / path).read_git(
                        "log", "--format=%s"
                    )
                ).splitlines()
                added_datasets[path] = commits
            else:
                other = True
        for path in modified:
            if (superds.pathobj / path / ".git").exists():
                last_commit = (
                    (await superds.read_git("ls-tree", "-z", "HEAD", "--", path))
                    .partition("\t")[0]
                    .split()[2]
                )
                commits = (
                    await AsyncDataset(superds.pathobj / path).read_git(
                        "log", "--format=%s", f"{last_commit}.."
                    )
                ).splitlines()
                modified_datasets[path] = commits
            else:
                other = True
        msg = ""
        if added_datasets:
            msg += f"{len(added_datasets)} added"
            if len(added_datasets) <= 3:
                msg += " (" + ", ".join(sorted(added_datasets.keys())) + ")"
        if modified_datasets:
            if msg:
                msg += ", "
            msg += f"{len(modified_datasets)} updated"
            if len(modified_datasets) <= 3:
                msg += " (" + ", ".join(sorted(modified_datasets.keys())) + ")"
        if other:
            if msg:
                msg += "; "
            msg += "other updates"
        if not msg:
            msg = "miscellaneous changes"
        for path, commits in sorted({**added_datasets, **modified_datasets}.items()):
            msg += f"\n\n{path}:"
            for c in commits:
                msg += f"\n - {c}"
        return msg
