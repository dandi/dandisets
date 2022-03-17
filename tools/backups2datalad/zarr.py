from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import os
from pathlib import Path
import sys
from typing import AsyncIterator, Optional, Set, Tuple
from urllib.parse import urlparse, urlunparse

import boto3
from botocore import UNSIGNED
from botocore.client import Config as BotoConfig
from dandi.dandiapi import RemoteZarrAsset, RemoteZarrEntry, ZarrEntryStat, ZarrListing
from datalad.api import Dataset
import dateutil.parser
import httpx
import trio

from .annex import AsyncAnnex
from .consts import ZARRS_REMOTE_PREFIX, ZARRS_REMOTE_UUID
from .util import (
    Config,
    MiniFuture,
    Remote,
    arequest,
    create_github_sibling,
    custom_commit_date,
    init_dataset,
    key2hash,
    log,
    maxdatetime,
    quantify,
)

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


CHECKSUM_FILE = ".zarr-checksum"


@dataclass
class ZarrReport:
    added: int = 0
    updated: int = 0
    deleted: int = 0
    checksum: bool = False

    def __bool__(self) -> bool:
        return bool(self.added or self.updated or self.deleted or self.checksum)

    def get_summary(self) -> str:
        msgparts = []
        if self.added:
            msgparts.append(f"{quantify(self.added, 'file')} added")
        if self.updated:
            msgparts.append(f"{quantify(self.updated, 'file')} updated")
        if self.deleted:
            msgparts.append(f"{quantify(self.deleted, 'file')} deleted")
        if self.checksum:
            msgparts.append("checksum updated")
        if not msgparts:
            msgparts.append("No changes")
        return ", ".join(msgparts)


@dataclass
class ZarrSyncer:
    api_url: str
    zarr_id: str
    repo: Path
    annex: AsyncAnnex
    s3client: httpx.AsyncClient
    backup_remote: Optional[str]
    checksum: str
    last_timestamp: Optional[datetime] = None
    extant_paths: Set[str] = field(default_factory=set)
    report: ZarrReport = field(default_factory=ZarrReport)

    async def run(self) -> None:
        async with aclosing(self.aiter_file_entries()) as ait:  # type: ignore[type-var]
            async for entry in ait:
                log.info("Zarr %s: %s: Syncing", self.zarr_id, entry)
                self.extant_paths.add(str(entry))
                bucket_url, st = await self.stat(entry)
                md5_digest = entry.get_digest().value
                self.last_timestamp = maxdatetime(self.last_timestamp, st.modified)
                dest = self.repo / str(entry)
                if dest.is_dir():
                    log.info(
                        "Zarr %s: %s: deleting conflicting directory at same path",
                        self.zarr_id,
                        entry,
                    )
                    await trio.to_thread.run_sync(self.rmtree, dest)
                else:
                    for ep in entry.parents:
                        pp = self.repo / str(ep)
                        if pp.is_file() or pp.is_symlink():
                            log.info(
                                "Zarr %s: %s: deleting conflicting file path %s",
                                self.zarr_id,
                                entry,
                                ep,
                            )
                            pp.unlink()
                            self.report.deleted += 1
                            break
                        elif pp.is_dir():
                            break
                to_update = False
                if not (dest.exists() or dest.is_symlink()):
                    log.info(
                        "Zarr %s: %s: Not in dataset; will add", self.zarr_id, entry
                    )
                    to_update = True
                    self.report.added += 1
                else:
                    log.debug(
                        "Zarr %s: %s: About to fetch hash from annex",
                        self.zarr_id,
                        entry,
                    )
                    if md5_digest == await self.get_annex_hash(dest):
                        log.info(
                            "Zarr %s: %s: File in dataset, and hash shows no"
                            " modification; will not update",
                            self.zarr_id,
                            entry,
                        )
                    else:
                        log.info(
                            "Zarr %s: %s: Asset in dataset, and hash shows"
                            " modification; will update",
                            self.zarr_id,
                            entry,
                        )
                        to_update = True
                        self.report.updated += 1
                if to_update:
                    dest.unlink(missing_ok=True)
                    key = await self.annex.mkkey(entry.name, st.size, md5_digest)
                    remotes = await self.annex.get_key_remotes(key)
                    await self.annex.from_key(key, str(entry))
                    await self.register_url(str(entry), key, bucket_url)
                    await self.register_url(
                        str(entry),
                        key,
                        f"{self.api_url}/zarr/{self.zarr_id}.zarr/{entry}",
                    )
                    if (
                        remotes is not None
                        and self.backup_remote is not None
                        and self.backup_remote not in remotes
                    ):
                        log.info(
                            "Zarr %s: %s: Not in backup remote %s",
                            self.zarr_id,
                            entry,
                            self.backup_remote,
                        )
        old_checksum: Optional[str]
        try:
            old_checksum = (self.repo / CHECKSUM_FILE).read_text().strip()
        except FileNotFoundError:
            old_checksum = None
        if old_checksum != self.checksum:
            log.info("Zarr %s: Updating checksum file", self.zarr_id)
            (self.repo / CHECKSUM_FILE).write_text(f"{self.checksum}\n")
            self.report.checksum = True
        await trio.to_thread.run_sync(self.prune_deleted)

    def rmtree(self, dirpath: Path) -> None:
        for p in list(dirpath.iterdir()):
            if p.is_dir():
                self.rmtree(p)
            else:
                log.info("Zarr %s: deleting %s", self.zarr_id, p)
                p.unlink()
                self.report.deleted += 1
        dirpath.rmdir()

    def prune_deleted(self) -> None:
        log.info("Zarr %s: deleting extra files", self.zarr_id)
        dirs = deque([self.repo])
        empty_dirs: deque[Path] = deque()
        while dirs:
            d = dirs.popleft()
            is_empty = True
            for p in list(d.iterdir()):
                path = p.relative_to(self.repo).as_posix()
                if d == self.repo and p.name in (
                    CHECKSUM_FILE,
                    ".datalad",
                    ".git",
                    ".gitattributes",
                ):
                    pass
                elif p.is_dir():
                    dirs.append(p)
                elif path not in self.extant_paths:
                    log.info("Zarr %s: deleting %s", self.zarr_id, path)
                    p.unlink()
                    self.report.deleted += 1
                else:
                    is_empty = False
            if is_empty and d != self.repo:
                empty_dirs.append(d)
        while empty_dirs:
            d = empty_dirs.popleft()
            d.rmdir()
            if d.parent != self.repo and not any(d.parent.iterdir()):
                empty_dirs.append(d.parent)

    async def aiter_file_entries(self) -> AsyncIterator[RemoteZarrEntry]:
        root = RemoteZarrEntry(client=None, zarr_id=self.zarr_id, parts=())
        dirs = deque([root])
        async with httpx.AsyncClient() as client:
            while dirs:
                d = dirs.popleft()
                path = "".join(p + "/" for p in d.parts)
                log.debug(
                    "Zarr %s: Getting listing for %s",
                    self.zarr_id,
                    path or "root directory",
                )
                try:
                    r = await arequest(
                        client, "GET", f"{self.api_url}/zarr/{self.zarr_id}.zarr/{path}"
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404 and d.is_root():
                        # Empty Zarr
                        return
                    else:
                        raise
                listing = ZarrListing.parse_obj(r.json())
                for name in listing.dirnames:
                    dirs.append(
                        d._get_subpath(
                            name, isdir=True, checksum=listing.checksums[name]
                        )
                    )
                for name in listing.filenames:
                    yield d._get_subpath(
                        name, isdir=False, checksum=listing.checksums[name]
                    )

    async def stat(self, entry: RemoteZarrEntry) -> Tuple[str, ZarrEntryStat]:
        # Returns the versioned AWS URL for the entry and the entry's size and
        # modified timestamp
        log.debug("Zarr %s: Querying S3 for details on %s", self.zarr_id, entry)
        r = await arequest(
            self.s3client,
            "HEAD",
            f"{self.api_url}/zarr/{self.zarr_id}.zarr/{entry}",
        )
        urlbits = urlparse(str(r.url))
        version_id = r.headers["x-amz-version-id"]
        aws_url = urlunparse(urlbits._replace(query=f"versionId={version_id}"))
        st = ZarrEntryStat(
            size=int(r.headers["Content-Length"]),
            modified=dateutil.parser.parse(r.headers["Last-Modified"]),
        )
        return (aws_url, st)

    async def get_annex_hash(self, filepath: Path) -> str:
        # OPT: do not bother checking or talking to annex --
        # shaves off about 20% of runtime on 000003, so let's just
        # not bother checking etc but judge from the resolved path to be
        # under (some) annex
        realpath = os.path.realpath(filepath)
        if os.path.islink(filepath) and ".git/annex/object" in realpath:
            return key2hash(os.path.basename(realpath))
        else:
            raise RuntimeError(f"{filepath} unexpectedly not under git-annex")

    async def register_url(self, path: str, key: str, url: str) -> None:
        log.info("Zarr %s: %s: Registering URL %s", self.zarr_id, path, url)
        await self.annex.register_url(key, url)


async def sync_zarr(
    asset: RemoteZarrAsset,
    checksum: str,
    dsdir: Path,
    config: Config,
    ts_fut: Optional[MiniFuture[Optional[datetime]]] = None,
) -> None:
    ds = Dataset(dsdir)
    if not ds.is_installed():
        await trio.to_thread.run_sync(init_zarr_dataset, ds, asset, config)
    async with trio.open_nursery() as nursery:
        delete_ts_fut: MiniFuture[Optional[datetime]] = MiniFuture()
        nursery.start_soon(
            get_latest_deletion, config.s3bucket, asset.zarr, delete_ts_fut
        )
        async with AsyncAnnex(dsdir, nursery, digest_type="MD5") as annex:
            async with httpx.AsyncClient() as s3client:
                zsync = ZarrSyncer(
                    api_url=asset.client.api_url,
                    zarr_id=asset.zarr,
                    repo=dsdir,
                    annex=annex,
                    s3client=s3client,
                    backup_remote=config.backup_remote,
                    checksum=checksum,
                )
                # Don't use `nursery.start_soon(zsync.run)`, as then the annex
                # and s3client would be closed before the run() finished.
                await zsync.run()
    report = zsync.report
    delete_ts = await delete_ts_fut.get()
    if report:
        summary = report.get_summary()
        log.info("Zarr %s: %s; committing", asset.zarr, summary)
        if zsync.last_timestamp is None:
            if delete_ts is None:
                commit_ts = asset.created
            else:
                commit_ts = delete_ts
        elif delete_ts is not None and zsync.last_timestamp < delete_ts:
            commit_ts = delete_ts
        else:
            commit_ts = zsync.last_timestamp
        await trio.to_thread.run_sync(
            save_and_push,
            ds,
            commit_ts,
            f"[backups2datalad] {summary}",
            config.jobs,
            config.zarr_gh_org is not None,
        )
        if ts_fut is not None:
            ts_fut.set(commit_ts)
    else:
        log.info("Zarr %s: no changes; not committing", asset.zarr)
        if ts_fut is not None:
            ts_fut.set(None)


def init_zarr_dataset(ds: Dataset, asset: RemoteZarrAsset, config: Config) -> None:
    remote: Optional[Remote]
    if config.zarr_backup_remote is not None:
        remote = Remote(
            name=config.zarr_backup_remote,
            prefix=ZARRS_REMOTE_PREFIX,
            uuid=ZARRS_REMOTE_UUID,
        )
    else:
        remote = None
    init_dataset(
        ds,
        desc=f"Zarr {asset.zarr}",
        commit_date=asset.created,
        backup_remote=remote,
        backend="MD5E",
        cfg_proc=None,
    )
    ds.repo.set_gitattributes([(CHECKSUM_FILE, {"annex.largefiles": "nothing"})])
    with custom_commit_date(asset.created):
        ds.save(message=f"Exclude {CHECKSUM_FILE} from git-annex")
    if config.zarr_gh_org is not None:
        create_github_sibling(
            ds,
            owner=config.zarr_gh_org,
            name=asset.zarr,
            backup_remote=config.zarr_backup_remote,
        )


def save_and_push(
    ds: Dataset, commit_date: datetime, commit_msg: str, jobs: int, push: bool
) -> None:
    with custom_commit_date(commit_date):
        ds.save(message=commit_msg)
    if push:
        ds.push(to="github", jobs=jobs)


async def get_latest_deletion(
    s3bucket: str, zarr_id: str, fut: MiniFuture[Optional[datetime]]
) -> None:
    ts = await trio.to_thread.run_sync(
        get_latest_delete_marker_timestamp, s3bucket, zarr_id
    )
    fut.set(ts)


def get_latest_delete_marker_timestamp(
    s3bucket: str, zarr_id: str
) -> Optional[datetime]:
    ts: Optional[datetime] = None
    client = boto3.client("s3", config=BotoConfig(signature_version=UNSIGNED))
    for page in client.get_paginator("list_object_versions").paginate(
        Bucket=s3bucket, Prefix=f"zarr/{zarr_id}/"
    ):
        for dm in page.get("DeleteMarkers", []):
            if dm["IsLatest"]:
                ts = maxdatetime(ts, dm["LastModified"])
    return ts
