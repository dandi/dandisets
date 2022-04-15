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
import sys
from typing import AsyncIterator, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

from dandi.dandiapi import AssetType, RemoteAsset, RemoteDandiset, RemoteZarrAsset
from dandi.exceptions import NotFoundError
from dandischema.models import DigestType
from datalad.api import Dataset, clone
import httpx
from identify.identify import tags_from_filename
import trio

from .annex import AsyncAnnex
from .util import (
    AssetTracker,
    Config,
    MiniFuture,
    Report,
    TextProcess,
    aiter,
    arequest,
    custom_commit_date,
    format_errors,
    key2hash,
    log,
    maxdatetime,
    open_git_annex,
    quantify,
)
from .zarr import sync_zarr

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class ToDownload:
    path: str
    url: str
    extra_urls: List[str]
    sha256_digest: str


@dataclass
class ZarrLink:
    zarr_dspath: Path
    timestamp_fut: MiniFuture[Optional[datetime]]
    asset_paths: List[str]


@dataclass
class Downloader:
    dandiset_id: str
    addurl: TextProcess
    repo: Path
    config: Config
    tracker: AssetTracker
    s3client: httpx.AsyncClient
    annex: AsyncAnnex
    nursery: trio.Nursery
    last_timestamp: Optional[datetime] = None
    report: Report = field(init=False, default_factory=Report)
    in_progress: Dict[str, ToDownload] = field(init=False, default_factory=dict)
    download_sender: trio.MemorySendChannel[ToDownload] = field(init=False)
    download_receiver: trio.MemoryReceiveChannel[ToDownload] = field(init=False)
    zarrs: Dict[str, ZarrLink] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.download_sender, self.download_receiver = trio.open_memory_channel(0)

    async def asset_loop(self, aia: AsyncIterator[Optional[RemoteAsset]]) -> None:
        now = datetime.now(timezone.utc)
        downloading = True
        async with self.download_sender:
            async for asset in aia:
                if asset is None:
                    break
                if downloading:
                    if asset.asset_type == AssetType.ZARR:
                        try:
                            zarr_digest = asset.get_digest().value
                        except NotFoundError:
                            log.info(
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
                            log.info(
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
                    log.info("%s: Will download in a future run", asset.path)
                    self.tracker.mark_future(asset)
                    if now - asset.created > timedelta(days=1):
                        log.error(
                            "%s: Asset created more than a day ago"
                            " but SHA256 digest has not yet been computed",
                            asset.path,
                        )
                        self.report.old_unhashed += 1

    async def process_asset(
        self,
        asset: RemoteAsset,
        sha256_digest: str,
        sender: trio.MemorySendChannel[ToDownload],
    ) -> None:
        async with sender:
            self.last_timestamp = maxdatetime(self.last_timestamp, asset.created)
            dest = self.repo / asset.path
            if not self.tracker.register_asset(asset, force=self.config.force):
                log.debug(
                    "%s: metadata unchanged; not taking any further action",
                    asset.path,
                )
                self.tracker.finish_asset(asset.path)
                return
            if not self.config.match_asset(asset.path):
                log.debug("%s: Skipping asset", asset.path)
                self.tracker.finish_asset(asset.path)
                return
            log.info("%s: Syncing", asset.path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            to_update = False
            if not (dest.exists() or dest.is_symlink()):
                log.info("%s: Not in dataset; will add", asset.path)
                to_update = True
                self.report.added += 1
            else:
                log.debug("%s: About to fetch hash from annex", asset.path)
                if sha256_digest == await self.get_annex_hash(dest):
                    log.info(
                        "%s: Asset in dataset, and hash shows no modification;"
                        " will not update",
                        asset.path,
                    )
                    self.tracker.finish_asset(asset.path)
                else:
                    log.info(
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
                    log.info(
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
                        and self.config.backup_remote is not None
                        and self.config.backup_remote not in remotes
                    ):
                        log.info(
                            "%s: Not in backup remote %s",
                            asset.path,
                            self.config.backup_remote,
                        )
                    self.tracker.finish_asset(asset.path)
                    self.report.registered += 1
                elif asset.size > (10 << 20):
                    raise RuntimeError(
                        f"{asset.path} identified as text but is {asset.size} bytes!"
                    )
                else:
                    log.info(
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
        elif self.config.zarr_target is None:
            raise RuntimeError(
                f"Zarr encountered in Dandiset {self.dandiset_id} but"
                " zarr_target not set"
            )
        else:
            zarr_dspath = self.config.zarr_target / asset.zarr
            ts_fut: MiniFuture[Optional[datetime]] = MiniFuture()
            self.nursery.start_soon(
                sync_zarr,
                asset,
                zarr_digest,
                zarr_dspath,
                self.config,
                ts_fut,
            )
            self.zarrs[asset.zarr] = ZarrLink(
                zarr_dspath=zarr_dspath,
                timestamp_fut=ts_fut,
                asset_paths=[asset.path],
            )

    async def get_file_bucket_url(self, asset: RemoteAsset) -> str:
        log.debug("%s: Fetching bucket URL", asset.path)
        aws_url = asset.get_content_url(self.config.content_url_regex)
        urlbits = urlparse(aws_url)
        key = urlbits.path.lstrip("/")
        log.debug("%s: About to query S3", asset.path)
        # aiobotocore doesn't work with trio, so we have to make the request
        # directly.  Fortunately, it's very simple.
        r = await arequest(
            self.s3client,
            "HEAD",
            f"https://{self.config.s3bucket}.s3.amazonaws.com/{key}",
        )
        r.raise_for_status()
        version_id = r.headers["x-amz-version-id"]
        log.debug("%s: Got bucket URL", asset.path)
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
            log.debug(
                "%s: Not under annex; calculating sha256 digest ourselves", filepath
            )
            return await asha256(filepath)

    async def feed_addurl(self) -> None:
        assert self.addurl.p.stdin is not None
        async with self.addurl.p.stdin:
            async with self.download_receiver:
                async for td in self.download_receiver:
                    self.in_progress[td.path] = td
                    log.info("%s: Downloading from %s", td.path, td.url)
                    await self.addurl.send(f"{td.url} {td.path}\n")
                log.debug("Done feeding URLs to addurl")

    async def read_addurl(self) -> None:
        async with aclosing(aiter(self.addurl)) as lineiter:  # type: ignore[type-var]
            async for line in lineiter:
                data = json.loads(line)
                if "byte-progress" in data:
                    # Progress message
                    log.info(
                        "%s: Downloaded %d / %s bytes (%s)",
                        data["action"]["file"],
                        data["byte-progress"],
                        data.get("total-size", "???"),
                        data.get("percent-progress", "??.??%"),
                    )
                elif not data["success"]:
                    log.error(
                        "%s: download failed:%s",
                        data["file"],
                        format_errors(data["error-messages"]),
                    )
                    self.in_progress.pop(data["file"])
                    self.report.failed += 1
                else:
                    path = data["file"]
                    key = data.get("key")
                    log.info("%s: Finished downloading (key = %s)", path, key)
                    self.report.downloaded += 1
                    dl = self.in_progress.pop(path)
                    self.tracker.finish_asset(dl.path)
                    self.nursery.start_soon(
                        self.check_unannexed_hash,
                        dl.path,
                        dl.sha256_digest,
                        name=f"check_unannexed_hash:{dl.path}",
                    )
        log.debug("Done reading from addurl")

    async def register_url(self, path: str, key: str, url: str) -> None:
        log.info("%s: Registering URL %s", path, url)
        await self.annex.register_url(key, url)

    async def check_unannexed_hash(self, asset_path: str, sha256_digest: str) -> None:
        annex_hash = await asha256(self.repo / asset_path)
        if sha256_digest != annex_hash:
            log.error(
                "%s: Hash mismatch!  Dandiarchive reports %s, local file has %s",
                asset_path,
                sha256_digest,
                annex_hash,
            )
            self.report.hash_mismatches += 1


async def async_assets(
    dandiset: RemoteDandiset, ds: Dataset, config: Config, tracker: AssetTracker
) -> Report:
    done_flag = trio.Event()
    total_report = Report()
    async with aclosing(  # type: ignore[type-var]
        aiterassets(dandiset, done_flag)
    ) as aia:
        while not done_flag.is_set():
            # We need an outer nursery to use for creating git-annex processes,
            # and we need an inner nursery for waiting for tasks to complete.
            # We can't combine the two, because the processes need to outlive
            # the `start_soon()` calls, so their scopes need to be outside the
            # inner nursery, which means we have to have an outer nursery as
            # well.
            async with trio.open_nursery() as annex_nursery:
                async with await open_git_annex(
                    annex_nursery,
                    "addurl",
                    "--batch",
                    "--with-files",
                    "--jobs",
                    str(config.jobs),
                    "--json",
                    "--json-error-messages",
                    "--json-progress",
                    "--raw",
                    path=ds.pathobj,
                ) as p, AsyncAnnex(
                    ds.pathobj, annex_nursery
                ) as annex, httpx.AsyncClient() as s3client:
                    try:
                        async with trio.open_nursery() as nursery:
                            dm = Downloader(
                                dandiset_id=dandiset.identifier,
                                addurl=p,
                                repo=ds.pathobj,
                                config=config,
                                tracker=tracker,
                                s3client=s3client,
                                annex=annex,
                                nursery=nursery,
                            )
                            nursery.start_soon(dm.asset_loop, aia)
                            nursery.start_soon(dm.feed_addurl)
                            nursery.start_soon(dm.read_addurl)
                    finally:
                        tracker.dump()

            timestamp = dm.last_timestamp
            for zarr_id, zarrlink in dm.zarrs.items():
                ts = await zarrlink.timestamp_fut.get()
                if ts is not None:
                    timestamp = maxdatetime(timestamp, ts)
                for asset_path in zarrlink.asset_paths:
                    dm.report.downloaded += 1
                    if not (ds.pathobj / asset_path).exists():
                        log.info("Zarr asset added at %s; cloning", asset_path)
                        dm.report.added += 1
                        if config.zarr_gh_org is not None:
                            src = "https://github.com/{config.zarr_gh_org}/{zarr_id}"
                        else:
                            assert config.zarr_target is not None
                            src = str(config.zarr_target / zarr_id)
                        await trio.to_thread.run_sync(
                            partial(clone, source=src, path=ds.pathobj / asset_path)
                        )
                    elif ts is not None:
                        log.info("Zarr asset modified at %s; updating", asset_path)
                        dm.report.updated += 1
                        zds = Dataset(ds.pathobj / asset_path)
                        if config.zarr_gh_org is not None:
                            await trio.to_thread.run_sync(
                                partial(zds.update, how="ff-only", sibling="github")
                            )
                        else:
                            await trio.to_thread.run_sync(
                                partial(zds.update, how="ff-only")
                            )

            if dandiset.version_id == "draft":
                if dm.report.registered or dm.report.downloaded:
                    log.info(
                        "%s registered, %s downloaded for this version segment;"
                        " committing",
                        quantify(dm.report.registered, "asset"),
                        quantify(dm.report.downloaded, "asset"),
                    )
                    if any(
                        r["state"] != "clean" for r in ds.status(result_renderer=None)
                    ):
                        log.info("Commiting changes")
                        assert timestamp is not None
                        with custom_commit_date(timestamp):
                            ds.save(message=dm.report.get_commit_message())
                        total_report.commits += 1
                else:
                    log.info(
                        "No assets downloaded for this version segment; not committing"
                    )
            total_report.update(dm.report)
    return total_report


async def aiterassets(
    dandiset: RemoteDandiset, done_flag: trio.Event
) -> AsyncIterator[Optional[RemoteAsset]]:
    last_ts: Optional[datetime] = None
    if dandiset.version_id == "draft":
        vs = [v for v in dandiset.get_versions() if v.identifier != "draft"]
        vs.sort(key=attrgetter("created"))
        versions = deque(vs)
    else:
        versions = deque()
    async with httpx.AsyncClient() as client:
        url: Optional[
            str
        ] = f"{dandiset.client.api_url}{dandiset.version_api_path}assets/?order=created"
        while url is not None:
            r = await arequest(client, "GET", url)
            data = r.json()
            for item in data["results"]:
                r = await arequest(
                    client,
                    "GET",
                    f"{dandiset.client.api_url}/assets/{item['asset_id']}/",
                )
                metadata = r.json()
                asset = RemoteAsset.from_data(dandiset, item, metadata)
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
                        "All assets up to creation of version %s found;"
                        " will commit soon",
                        versions[0].identifier,
                    )
                    versions.popleft()
                    yield None
                last_ts = asset.created
                yield asset
            url = data.get("next")
    log.info("Finished getting assets from API")
    done_flag.set()


async def asha256(path: Path) -> str:
    log.debug("Starting to compute sha256 digest of %s", path)
    tp = trio.Path(path)
    digester = hashlib.sha256()
    async with await tp.open("rb") as fp:
        while True:
            blob = await fp.read(65535)
            if blob == b"":
                break
            digester.update(blob)
    log.debug("Finished computing sha256 digest of %s", path)
    return digester.hexdigest()
