from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
import os
from pathlib import Path
import subprocess
import sys
from typing import TYPE_CHECKING, AsyncIterator, Iterator, Optional, Set, Tuple, cast
from urllib.parse import quote

from aiobotocore.session import get_session
import anyio
from botocore import UNSIGNED
from botocore.client import Config as BotoConfig
from dandi.dandiapi import RemoteZarrAsset
from datalad.api import Dataset
from pydantic import BaseModel

from .aioutil import MiniFuture
from .annex import AsyncAnnex
from .config import Config, ResourceConfig
from .util import (
    create_github_sibling,
    custom_commit_date,
    init_dataset,
    is_meta_file,
    key2hash,
    log,
    maxdatetime,
    quantify,
)

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client


OLD_CHECKSUM_FILE = Path(".zarr-checksum")
CHECKSUM_FILE = Path(".dandi", "zarr-checksum")

SYNC_FILE = Path(".dandi", "s3sync.json")


class SyncData(BaseModel):
    bucket: str
    prefix: str
    last_modified: Optional[datetime]


@dataclass
class ZarrEntry:
    path: str
    size: int
    md5_digest: str
    last_modified: datetime
    bucket_url: str

    def __str__(self) -> str:
        return self.path

    @property
    def parts(self) -> Tuple[str, ...]:
        return tuple(self.path.split("/"))

    @property
    def name(self) -> str:
        return self.parts[-1]

    @property
    def parents(self) -> Iterator[str]:
        parts = self.parts
        while parts:
            parts = parts[:-1]
            yield "/".join(parts)


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
    s3bucket: str
    s3prefix: str = field(init=False)
    backup_remote: Optional[str]
    checksum: str
    last_timestamp: Optional[datetime] = None
    report: ZarrReport = field(default_factory=ZarrReport)

    def __post_init__(self) -> None:
        self.s3prefix = f"zarr/{self.zarr_id}/"

    async def run(self) -> None:
        last_sync = self.read_sync_file()
        async with aclosing(self.annex.list_files()) as fileiter:
            local_paths = {f async for f in fileiter if not is_meta_file(f)}
        async with get_session().create_client(
            "s3", config=BotoConfig(signature_version=UNSIGNED)
        ) as client:
            if not await self.needs_sync(client, last_sync, local_paths):
                log.info("Zarr %s: backup up to date", self.zarr_id)
                return
            log.info("Zarr %s: sync needed", self.zarr_id)
            async with aclosing(self.aiter_file_entries(client)) as ait:
                async for entry in ait:
                    if is_meta_file(str(entry)):
                        raise RuntimeError(
                            f"Zarr {self.zarr_id} contains file at meta path"
                            f" {str(entry)!r}"
                        )
                    log.info("Zarr %s: %s: Syncing", self.zarr_id, entry)
                    local_paths.discard(str(entry))
                    if last_sync is not None and entry.last_modified < last_sync:
                        log.info(
                            "Zarr %s: %s: file not modified since last backup",
                            self.zarr_id,
                            entry,
                        )
                        continue
                    dest = self.repo / str(entry)
                    if dest.is_dir():
                        # File path is replacing a directory, which needs to be
                        # deleted
                        log.info(
                            "Zarr %s: %s: deleting conflicting directory at same path",
                            self.zarr_id,
                            entry,
                        )
                        await anyio.to_thread.run_sync(self.rmtree, dest, local_paths)
                    else:
                        for ep in entry.parents:
                            pp = self.repo / ep
                            if pp.is_file() or pp.is_symlink():
                                # Annexed file at parent path of `entry` needs to
                                # be replaced with a directory
                                log.info(
                                    "Zarr %s: %s: deleting conflicting file path %s",
                                    self.zarr_id,
                                    entry,
                                    ep,
                                )
                                pp.unlink()
                                local_paths.discard(ep)
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
                        if entry.md5_digest == self.get_annex_hash(dest):
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
                        key = await self.annex.mkkey(
                            entry.name, entry.size, entry.md5_digest
                        )
                        remotes = await self.annex.get_key_remotes(key)
                        await self.annex.from_key(key, str(entry))
                        await self.register_url(str(entry), key, entry.bucket_url)
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
            (self.repo / CHECKSUM_FILE).parent.mkdir(exist_ok=True)
            (self.repo / CHECKSUM_FILE).write_text(f"{self.checksum}\n")
            self.report.checksum = True
        # Remove a possibly still present previous location for the checksum file
        if (self.repo / OLD_CHECKSUM_FILE).exists():
            (self.repo / OLD_CHECKSUM_FILE).unlink()
        self.write_sync_file()
        await anyio.to_thread.run_sync(self.prune_deleted, local_paths)

    def read_sync_file(self) -> Optional[datetime]:
        try:
            data = SyncData.parse_file(self.repo / SYNC_FILE)
        except FileNotFoundError:
            return None
        if data.bucket != self.s3bucket:
            raise RuntimeError(
                f"Bucket {self.s3bucket!r} for Zarr {self.zarr_id} does not"
                f" match bucket in {SYNC_FILE} ({data.bucket!r})"
            )
        if data.prefix != self.s3prefix:
            raise RuntimeError(
                f"Key prefix {self.s3prefix!r} for Zarr {self.zarr_id} does not"
                f" match prefix in {SYNC_FILE} ({data.prefix!r})"
            )
        return data.last_modified

    def write_sync_file(self) -> None:
        data = SyncData(
            bucket=self.s3bucket,
            prefix=self.s3prefix,
            last_modified=self.last_timestamp,
        )
        (self.repo / SYNC_FILE).parent.mkdir(exist_ok=True)
        (self.repo / SYNC_FILE).write_text(data.json(indent=4) + "\n")

    async def needs_sync(
        self, client: S3Client, last_sync: Optional[datetime], local_paths: Set[str]
    ) -> bool:
        if last_sync is None:
            return True
        local_paths = local_paths.copy()
        # We fetch a list of all objects from the server here (using
        # `list_objects_v2`) in order to decide whether to sync, and then the
        # actual syncing fetches all objects again using
        # `list_object_versions`.  The latter endpoint is the only one that
        # includes version IDs, yet it's also considerably slower than
        # `list_objects_v2`, so we try to optimize for the presumed-common case
        # of Zarrs rarely being modified.
        leadlen = len(self.s3prefix)
        async with aclosing(self.aiter_objects(client)) as ao:
            async for obj in ao:
                path = obj["Key"][leadlen:]
                try:
                    local_paths.remove(path)
                except KeyError:
                    log.info(
                        "Zarr %s: %s on server but not in backup",
                        self.zarr_id,
                        path,
                    )
                    return True
                if obj["LastModified"] > last_sync:
                    log.info(
                        "Zarr %s: %s was modified on server at %s, after last"
                        " sync at %s",
                        self.zarr_id,
                        path,
                        obj["LastModified"],
                        last_sync,
                    )
                    return True
        if local_paths:
            log.info(
                "Zarr %s: %s in local backup but no longer on server",
                self.zarr_id,
                quantify(len(local_paths), "file"),
            )
            return True
        return False

    def rmtree(self, dirpath: Path, local_paths: Set[str]) -> None:
        for p in list(dirpath.iterdir()):
            if p.is_dir():
                self.rmtree(p, local_paths)
            else:
                log.info("Zarr %s: deleting %s", self.zarr_id, p)
                p.unlink()
                self.report.deleted += 1
                local_paths.discard(p.relative_to(self.repo).as_posix())
        dirpath.rmdir()

    def prune_deleted(self, local_paths: Set[str]) -> None:
        log.info("Zarr %s: deleting extra files", self.zarr_id)
        for path in local_paths:
            log.info("Zarr %s: deleting %s", self.zarr_id, path)
            p = self.repo / path
            p.unlink(missing_ok=True)
            self.report.deleted += 1
            d = p.parent
            while d != self.repo and not any(d.iterdir()):
                d.rmdir()
                d = d.parent
        log.info("Zarr %s: finished deleting extra files", self.zarr_id)

    async def aiter_objects(self, client: S3Client) -> AsyncIterator[dict]:
        async for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=self.s3bucket, Prefix=self.s3prefix
        ):
            for obj in page.get("Contents", []):
                yield cast(dict, obj)

    async def aiter_file_entries(self, client: S3Client) -> AsyncIterator[ZarrEntry]:
        leadlen = len(self.s3prefix)
        async for page in client.get_paginator("list_object_versions").paginate(
            Bucket=self.s3bucket, Prefix=self.s3prefix
        ):
            for v in page.get("Versions", []):
                if v["IsLatest"]:
                    self.last_timestamp = maxdatetime(
                        self.last_timestamp, v["LastModified"]
                    )
                    yield ZarrEntry(
                        path=v["Key"][leadlen:],
                        size=v["Size"],
                        md5_digest=v["ETag"].strip('"'),
                        last_modified=v["LastModified"],
                        bucket_url=f"https://{self.s3bucket}.s3.amazonaws.com/{quote(v['Key'])}?versionId={v['VersionId']}",
                    )
            for dm in page.get("DeleteMarkers", []):
                if dm["IsLatest"]:
                    self.last_timestamp = maxdatetime(
                        self.last_timestamp, dm["LastModified"]
                    )

    def get_annex_hash(self, filepath: Path) -> str:
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
    limit: Optional[anyio.CapacityLimiter] = None,
    ts_fut: Optional[MiniFuture[Optional[datetime]]] = None,
) -> None:
    if limit is None:
        # For use when calling sync_zarr() directly from a test, where we can't
        # construct a CapacityLimiter outside of an async context.
        limit = anyio.CapacityLimiter(1)
    async with limit:
        assert config.zarrs is not None
        ds = Dataset(dsdir)
        if not ds.is_installed():
            await anyio.to_thread.run_sync(init_zarr_dataset, ds, asset, config.zarrs)
        async with anyio.create_task_group() as nursery:
            async with AsyncAnnex(dsdir, nursery, digest_type="MD5") as annex:
                if (r := config.zarrs.remote) is not None:
                    backup_remote = r.name
                else:
                    backup_remote = None
                zsync = ZarrSyncer(
                    api_url=asset.client.api_url,
                    zarr_id=asset.zarr,
                    repo=dsdir,
                    annex=annex,
                    s3bucket=config.s3bucket,
                    backup_remote=backup_remote,
                    checksum=checksum,
                )
                # Don't use `nursery.start_soon(zsync.run)`, as then the annex
                # would be closed before the run() finished.
                await zsync.run()
        report = zsync.report
        if report:
            summary = report.get_summary()
            log.info("Zarr %s: %s; committing", asset.zarr, summary)
            if zsync.last_timestamp is None:
                commit_ts = asset.created
            else:
                commit_ts = zsync.last_timestamp
            await anyio.to_thread.run_sync(
                save, ds, commit_ts, f"[backups2datalad] {summary}"
            )
            log.debug("Zarr %s: Commit made", asset.zarr)
            log.debug("Zarr %s: Running `git gc`", asset.zarr)
            try:
                await anyio.run_process(
                    ["git", "gc"], cwd=ds.path, stdout=None, stderr=None
                )
            except subprocess.CalledProcessError as e:
                if e.returncode == 128:
                    log.warning("`git gc` in %s exited with code 128", ds.path)
                else:
                    raise
            log.debug("Zarr %s: Finished running `git gc`", asset.zarr)
            if config.zarr_gh_org is not None:
                log.debug("Zarr %s: Pushing to GitHub", asset.zarr)
                await anyio.to_thread.run_sync(
                    partial(ds.push, to="github", jobs=config.jobs, data="nothing")
                )
                log.debug("Zarr %s: Finished pushing to GitHub", asset.zarr)
            if ts_fut is not None:
                ts_fut.set(commit_ts)
        else:
            log.info("Zarr %s: no changes; not committing", asset.zarr)
            if ts_fut is not None:
                ts_fut.set(None)


def init_zarr_dataset(
    ds: Dataset, asset: RemoteZarrAsset, zcfg: ResourceConfig
) -> None:
    init_dataset(
        ds,
        desc=f"Zarr {asset.zarr}",
        commit_date=asset.created,
        backup_remote=zcfg.remote,
        backend="MD5E",
        cfg_proc=None,
    )
    log.debug("Zarr %s: Excluding .dandi/ from git-annex", asset.zarr)
    ds.repo.set_gitattributes(
        [("*", {"annex.largefiles": "nothing"})], attrfile=".dandi/.gitattributes"
    )
    with custom_commit_date(asset.created):
        ds.save(message="Exclude .dandi/ from git-annex")
    if (zgh := zcfg.github_org) is not None:
        log.debug("Zarr %s: Creating GitHub sibling", asset.zarr)
        create_github_sibling(ds, owner=zgh, name=asset.zarr, backup_remote=zcfg.remote)
    log.debug("Zarr %s: Finished initializing dataset", asset.zarr)


def save(ds: Dataset, commit_date: datetime, commit_msg: str) -> None:
    with custom_commit_date(commit_date):
        ds.save(message=commit_msg)
