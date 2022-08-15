from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
import hashlib
import json
from operator import attrgetter
import os.path
from pathlib import Path, PurePosixPath
import subprocess
import sys
from typing import AsyncGenerator, AsyncIterator, Optional
from urllib.parse import urlparse, urlunparse

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from dandi.dandiapi import AssetType, RemoteAsset, RemoteZarrAsset
from dandi.exceptions import NotFoundError
from dandischema.models import DigestType
import datalad
from datalad.api import clone
import httpx
from identify.identify import tags_from_filename

from .adandi import RemoteDandiset
from .adataset import AssetsState, AsyncDataset, DatasetStats
from .aioutil import TextProcess, arequest, aruncmd, open_git_annex
from .annex import AsyncAnnex
from .config import BackupConfig
from .consts import USER_AGENT
from .logging import PrefixedLogger, log
from .manager import Manager
from .util import (
    AssetTracker,
    UnexpectedChangeError,
    format_errors,
    key2hash,
    maxdatetime,
    quantify,
)
from .zarr import ZarrLink, sync_zarr

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class ToDownload:
    path: str
    url: str
    extra_urls: list[str]
    sha256_digest: str


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
    zarr_stats: dict[str, DatasetStats] = field(default_factory=dict)

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
        errors: list[str] = []
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
class Downloader:
    dandiset_id: str
    addurl: TextProcess
    repo: Path
    manager: Manager
    tracker: AssetTracker
    s3client: httpx.AsyncClient
    annex: AsyncAnnex
    nursery: anyio.abc.TaskGroup
    error_on_change: bool = False
    last_timestamp: Optional[datetime] = None
    report: Report = field(init=False, default_factory=Report)
    in_progress: dict[str, ToDownload] = field(init=False, default_factory=dict)
    download_sender: MemoryObjectSendStream[ToDownload] = field(init=False)
    download_receiver: MemoryObjectReceiveStream[ToDownload] = field(init=False)
    zarrs: dict[str, ZarrLink] = field(init=False, default_factory=dict)
    need_add: list[str] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        (
            self.download_sender,
            self.download_receiver,
        ) = anyio.create_memory_object_stream(0)

    @property
    def config(self) -> BackupConfig:
        return self.manager.config

    @property
    def log(self) -> PrefixedLogger:
        return self.manager.log

    async def asset_loop(self, aia: AsyncIterator[Optional[RemoteAsset]]) -> None:
        now = datetime.now(timezone.utc)
        downloading = True
        async with self.download_sender:
            async for asset in aia:
                if asset is None:
                    break
                self.tracker.remote_assets.add(asset.path)
                if downloading:
                    if asset.asset_type == AssetType.ZARR:
                        try:
                            zarr_digest = asset.get_digest().value
                        except NotFoundError:
                            self.log.info(
                                "%s: Zarr checksum has not been computed yet;"
                                " not downloading any more assets",
                                asset.path,
                            )
                            downloading = False
                        else:
                            self.nursery.start_soon(
                                self.process_zarr,
                                asset,
                                zarr_digest,
                                name=f"process_zarr:{asset.path}",
                            )
                    else:
                        try:
                            sha256_digest = asset.get_raw_digest(DigestType.sha2_256)
                        except NotFoundError:
                            self.log.info(
                                "%s: SHA256 has not been computed yet;"
                                " not downloading any more assets",
                                asset.path,
                            )
                            downloading = False
                        else:
                            self.nursery.start_soon(
                                self.process_asset,
                                asset,
                                sha256_digest,
                                self.download_sender.clone(),
                                name=f"process_asset:{asset.path}",
                            )
                # Not `else`, as we want to "fall through" if `downloading`
                # is negated above.
                if not downloading:
                    self.log.info("%s: Will download in a future run", asset.path)
                    self.tracker.mark_future(asset)
                    if (
                        asset.asset_type == AssetType.BLOB
                        and now - asset.created > timedelta(days=1)
                    ):
                        try:
                            asset.get_raw_digest(DigestType.sha2_256)
                        except NotFoundError:
                            self.log.error(
                                "%s: Asset created more than a day ago"
                                " but SHA256 digest has not yet been computed",
                                asset.path,
                            )
                            self.report.old_unhashed += 1

    async def process_asset(
        self,
        asset: RemoteAsset,
        sha256_digest: str,
        sender: MemoryObjectSendStream[ToDownload],
    ) -> None:
        async with sender:
            self.last_timestamp = maxdatetime(self.last_timestamp, asset.created)
            dest = self.repo / asset.path
            if not self.tracker.register_asset(asset, force=self.config.force):
                self.log.debug(
                    "%s: metadata unchanged; not taking any further action",
                    asset.path,
                )
                self.tracker.finish_asset(asset.path)
                return
            if not self.config.match_asset(asset.path):
                self.log.debug("%s: Skipping asset", asset.path)
                self.tracker.finish_asset(asset.path)
                return
            if self.error_on_change:
                raise UnexpectedChangeError(
                    f"Metadata for asset {asset.path} was changed/added but"
                    " draft timestamp was not updated on server"
                )
            self.log.info("%s: Syncing", asset.path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            to_update = False
            if not (dest.exists() or dest.is_symlink()):
                self.log.info("%s: Not in dataset; will add", asset.path)
                to_update = True
                self.report.added += 1
            else:
                self.log.debug("%s: About to fetch hash from annex", asset.path)
                if sha256_digest == await self.get_annex_hash(dest):
                    self.log.info(
                        "%s: Asset in dataset, and hash shows no modification;"
                        " will not update",
                        asset.path,
                    )
                    self.tracker.finish_asset(asset.path)
                else:
                    self.log.info(
                        "%s: Asset in dataset, and hash shows modification;"
                        " will update",
                        asset.path,
                    )
                    to_update = True
                    self.report.updated += 1
            if to_update:
                bucket_url = await self.get_file_bucket_url(asset)
                dest.unlink(missing_ok=True)
                if "text" not in tags_from_filename(asset.path):
                    self.log.info(
                        "%s: File is binary; registering key with git-annex", asset.path
                    )
                    key = await self.annex.mkkey(
                        PurePosixPath(asset.path).name, asset.size, sha256_digest
                    )
                    await self.annex.from_key(key, asset.path)
                    await self.register_url(asset.path, key, bucket_url)
                    await self.register_url(asset.path, key, asset.base_download_url)
                    remotes = await self.annex.get_key_remotes(key)
                    if (
                        remotes is not None
                        and self.config.dandisets.remote is not None
                        and self.config.dandisets.remote.name not in remotes
                    ):
                        self.log.info(
                            "%s: Not in backup remote %r",
                            asset.path,
                            self.config.dandisets.remote.name,
                        )
                    self.tracker.finish_asset(asset.path)
                    self.report.registered += 1
                elif asset.size > (10 << 20):
                    raise RuntimeError(
                        f"{asset.path} identified as text but is {asset.size} bytes!"
                    )
                else:
                    self.log.info(
                        "%s: File is text; sending off for download from %s",
                        asset.path,
                        bucket_url,
                    )
                    await sender.send(
                        ToDownload(
                            path=asset.path,
                            url=bucket_url,
                            extra_urls=[asset.base_download_url],
                            sha256_digest=sha256_digest,
                        )
                    )

    async def process_zarr(self, asset: RemoteZarrAsset, zarr_digest: str) -> None:
        self.tracker.register_asset(asset, force=self.config.force)
        self.tracker.finish_asset(asset.path)
        # In case the Zarr is empty:
        self.last_timestamp = maxdatetime(self.last_timestamp, asset.created)
        if asset.zarr in self.zarrs:
            self.zarrs[asset.zarr].asset_paths.append(asset.path)
        elif self.config.zarr_root is None:
            raise RuntimeError(
                f"Zarr encountered in Dandiset {self.dandiset_id} but"
                " Zarr backups not configured in config file"
            )
        else:
            zarr_dspath = self.config.zarr_root / asset.zarr
            zl = ZarrLink(
                zarr_dspath=zarr_dspath,
                timestamp=None,
                asset_paths=[asset.path],
            )
            self.nursery.start_soon(
                partial(sync_zarr, link=zl, error_on_change=self.error_on_change),
                asset,
                zarr_digest,
                zarr_dspath,
                self.manager.with_sublogger(f"Zarr {asset.zarr}"),
            )
            self.zarrs[asset.zarr] = zl

    async def get_file_bucket_url(self, asset: RemoteAsset) -> str:
        self.log.debug("%s: Fetching bucket URL", asset.path)
        aws_url = asset.get_content_url(self.config.content_url_regex)
        urlbits = urlparse(aws_url)
        key = urlbits.path.lstrip("/")
        self.log.debug("%s: About to query S3", asset.path)
        r = await arequest(
            self.s3client,
            "HEAD",
            f"https://{self.config.s3bucket}.s3.amazonaws.com/{key}",
        )
        r.raise_for_status()
        version_id = r.headers["x-amz-version-id"]
        self.log.debug("%s: Got bucket URL", asset.path)
        return urlunparse(urlbits._replace(query=f"versionId={version_id}"))

    async def get_annex_hash(self, filepath: Path) -> str:
        # OPT: do not bother checking or talking to annex --
        # shaves off about 20% of runtime on 000003, so let's just
        # not bother checking etc but judge from the resolved path to be
        # under (some) annex
        realpath = os.path.realpath(filepath)
        if os.path.islink(filepath) and ".git/annex/object" in realpath:
            return key2hash(os.path.basename(realpath))
        else:
            self.log.debug(
                "%s: Not under annex; calculating sha256 digest ourselves", filepath
            )
            return await self.asha256(filepath)

    async def feed_addurl(self) -> None:
        assert self.addurl.p.stdin is not None
        async with self.addurl.p.stdin:
            async with self.download_receiver:
                async for td in self.download_receiver:
                    self.in_progress[td.path] = td
                    self.log.info("%s: Downloading from %s", td.path, td.url)
                    await self.addurl.send(f"{td.url} {td.path}\n")
                self.log.debug("Done feeding URLs to addurl")

    async def read_addurl(self) -> None:
        async with aclosing(self.addurl) as lineiter:
            async for line in lineiter:
                data = json.loads(line)
                if "byte-progress" in data:
                    # Progress message
                    self.log.info(
                        "%s: Downloaded %d / %s bytes (%s)",
                        data["action"]["file"],
                        data["byte-progress"],
                        data.get("total-size", "???"),
                        data.get("percent-progress", "??.??%"),
                    )
                elif not data["success"]:
                    msg = format_errors(data["error-messages"])
                    self.log.error("%s: download failed:%s", data["file"], msg)
                    self.in_progress.pop(data["file"])
                    if "exited 123" in msg:
                        self.log.info(
                            "Will try `git add`ing %s manually later", data["file"]
                        )
                        self.need_add.append(data["file"])
                    else:
                        self.report.failed += 1
                else:
                    path = data["file"]
                    key = data.get("key")
                    self.log.info("%s: Finished downloading (key = %s)", path, key)
                    self.report.downloaded += 1
                    dl = self.in_progress.pop(path)
                    self.tracker.finish_asset(dl.path)
                    self.nursery.start_soon(
                        self.check_unannexed_hash,
                        dl.path,
                        dl.sha256_digest,
                        name=f"check_unannexed_hash:{dl.path}",
                    )
        self.log.debug("Done reading from addurl")

    async def register_url(self, path: str, key: str, url: str) -> None:
        self.log.info("%s: Registering URL %s", path, url)
        await self.annex.register_url(key, url)

    async def check_unannexed_hash(self, asset_path: str, sha256_digest: str) -> None:
        annex_hash = await self.asha256(self.repo / asset_path)
        if sha256_digest != annex_hash:
            self.log.error(
                "%s: Hash mismatch!  Dandiarchive reports %s, local file has %s",
                asset_path,
                sha256_digest,
                annex_hash,
            )
            self.report.hash_mismatches += 1

    async def asha256(self, path: Path) -> str:
        self.log.debug("Starting to compute sha256 digest of %s", path)
        tp = anyio.Path(path)
        digester = hashlib.sha256()
        async with await tp.open("rb") as fp:
            while True:
                blob = await fp.read(65535)
                if blob == b"":
                    break
                digester.update(blob)
        self.log.debug("Finished computing sha256 digest of %s", path)
        return digester.hexdigest()


async def async_assets(
    dandiset: RemoteDandiset,
    ds: AsyncDataset,
    manager: Manager,
    tracker: AssetTracker,
    error_on_change: bool = False,
) -> Report:
    if datalad.support.external_versions.external_versions["cmd:annex"] < "10.20220724":
        raise RuntimeError(
            "git-annex does not support annex.alwayscompact=false;"
            " v10.20220724 required"
        )
    done_flag = anyio.Event()
    total_report = Report()
    async with aclosing(aiterassets(dandiset, done_flag)) as aia:
        while not done_flag.is_set():
            try:
                async with await open_git_annex(
                    "addurl",
                    "-c",
                    "annex.alwayscompact=false",
                    "--batch",
                    "--with-files",
                    "--jobs",
                    str(manager.config.jobs),
                    "--json",
                    "--json-error-messages",
                    "--json-progress",
                    "--raw",
                    path=ds.pathobj,
                ) as p, AsyncAnnex(ds.pathobj) as annex, httpx.AsyncClient(
                    headers={"User-Agent": USER_AGENT}
                ) as s3client, anyio.create_task_group() as nursery:
                    dm = Downloader(
                        dandiset_id=dandiset.identifier,
                        addurl=p,
                        repo=ds.pathobj,
                        manager=manager,
                        tracker=tracker,
                        s3client=s3client,
                        annex=annex,
                        nursery=nursery,
                        error_on_change=error_on_change,
                    )
                    nursery.start_soon(dm.asset_loop, aia)
                    nursery.start_soon(dm.feed_addurl)
                    nursery.start_soon(dm.read_addurl)
            finally:
                tracker.dump()

            for fpath in dm.need_add:
                manager.log.info("Manually running `git add %s`", fpath)
                try:
                    await ds.call_git("add", fpath)
                except subprocess.CalledProcessError:
                    manager.log.error("Manual `git add %s` failed", fpath)
                    dm.report.failed += 1

            timestamp = dm.last_timestamp
            for zarr_id, zarrlink in dm.zarrs.items():
                # We've left the task group, so all of the Zarr tasks have
                # finished and set the timestamps & stats in their links
                assert zarrlink.stats is not None
                total_report.zarr_stats[zarr_id] = zarrlink.stats
                ts = zarrlink.timestamp
                if ts is not None:
                    timestamp = maxdatetime(timestamp, ts)
                for asset_path in zarrlink.asset_paths:
                    if not (ds.pathobj / asset_path).exists():
                        if error_on_change:
                            raise UnexpectedChangeError(
                                f"Zarr asset added at {asset_path} but draft"
                                " timestamp was not updated on server"
                            )
                        manager.log.info("Zarr asset added at %s; cloning", asset_path)
                        dm.report.downloaded += 1
                        dm.report.added += 1
                        if manager.config.zarr_gh_org is not None:
                            src = (
                                "https://github.com/"
                                f"{manager.config.zarr_gh_org}/{zarr_id}"
                            )
                        else:
                            assert manager.config.zarr_root is not None
                            src = str(manager.config.zarr_root / zarr_id)
                        await anyio.to_thread.run_sync(
                            partial(clone, source=src, path=ds.pathobj / asset_path)
                        )
                        if manager.config.zarr_gh_org is not None:
                            await aruncmd(
                                "git",
                                "remote",
                                "rename",
                                "origin",
                                "github",
                                cwd=ds.pathobj / asset_path,
                            )
                        manager.log.debug("Finished cloning Zarr to %s", asset_path)
                    elif ts is not None:
                        manager.log.info(
                            "Zarr asset modified at %s; updating", asset_path
                        )
                        dm.report.downloaded += 1
                        dm.report.updated += 1
                        zds = AsyncDataset(ds.pathobj / asset_path)
                        if manager.config.zarr_gh_org is not None:
                            await zds.update(how="ff-only", sibling="github")
                        else:
                            await zds.update(how="ff-only")
                        manager.log.debug("Finished updating Zarr at %s", asset_path)

            if dandiset.version_id == "draft":
                if dm.report.registered or dm.report.downloaded:
                    manager.log.info(
                        "%s registered, %s downloaded for this version segment;"
                        " committing",
                        quantify(dm.report.registered, "asset"),
                        quantify(dm.report.downloaded, "asset"),
                    )
                    assert timestamp is not None
                    if done_flag.is_set():
                        ts = dandiset.version.modified
                    else:
                        ts = timestamp
                    ds.set_assets_state(AssetsState(timestamp=ts))
                    manager.log.debug("Checking whether repository is dirty ...")
                    if await ds.is_unclean():
                        manager.log.info("Committing changes")
                        await ds.save(
                            message=dm.report.get_commit_message(),
                            commit_date=timestamp,
                        )
                        manager.log.debug("Commit made")
                        total_report.commits += 1
                    else:
                        manager.log.debug("Repository is clean")
                else:
                    if done_flag.is_set():
                        ds.set_assets_state(
                            AssetsState(timestamp=dandiset.version.modified)
                        )
                    manager.log.info(
                        "No assets downloaded for this version segment; not committing"
                    )
            else:
                ds.set_assets_state(AssetsState(timestamp=dandiset.version.created))
            total_report.update(dm.report)
    return total_report


async def aiterassets(
    dandiset: RemoteDandiset, done_flag: anyio.Event
) -> AsyncGenerator[Optional[RemoteAsset], None]:
    last_ts: Optional[datetime] = None
    if dandiset.version_id == "draft":
        vs = [v async for v in dandiset.aget_versions(include_draft=False)]
        vs.sort(key=attrgetter("created"))
        versions = deque(vs)
    else:
        versions = deque()
    async for asset in dandiset.aget_assets():
        assert last_ts is None or last_ts <= asset.created, (
            f"Asset {asset.path} created at {asset.created} but"
            f" returned after an asset created at {last_ts}!"
        )
        if (
            versions
            and (last_ts is None or last_ts < versions[0].created)
            and asset.created >= versions[0].created
        ):
            log.info(
                "Dandiset %s: All assets up to creation of version %s found;"
                " will commit soon",
                dandiset.identifier,
                versions[0].identifier,
            )
            versions.popleft()
            yield None
        last_ts = asset.created
        yield asset
    log.info("Dandiset %s: Finished getting assets from API", dandiset.identifier)
    done_flag.set()
