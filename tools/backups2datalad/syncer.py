import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
from operator import attrgetter
import os.path
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Set, Union, cast

from dandi.dandiapi import RemoteAsset, RemoteDandiset
from dandi.exceptions import NotFoundError

# from fscacher import PersistentCache
from dandi.support.digests import Digester
from dandischema.models import DigestType
from datalad.api import Dataset
from datalad.support.annexrepo import BatchedAnnexes
from datalad.support.json_py import dump

from . import log
from .asynchron import download_urls
from .util import Config, asset2dict, dataset_files, quantify


@dataclass
class ToDownload:
    path: str
    url: str
    extra_urls: List[str]
    sha256_digest: str


@dataclass
class Syncer:
    config: Config
    dandiset: RemoteDandiset
    ds: Dataset
    added: int = 0
    updated: int = 0
    deleted: int = 0
    #: Paths of files found when the syncing started, minus the paths for any
    #: assets downloaded during syncing
    local_assets: Set[str] = field(init=False)
    #: Metadata of assets downloaded during the sync
    asset_metadata: List[dict] = field(init=False, default_factory=list)
    #: Asset metadata from previous sync, as a mapping from asset paths to
    #: metadata
    saved_metadata: Dict[str, dict] = field(init=False, default_factory=dict)
    #: Paths of assets that are not being downloaded this run due to a lack of
    #: SHA256 digests
    future_assets: Set[str] = field(init=False, default_factory=set)
    digester: Digester = field(init=False)
    # cache: PersistentCache = field(init=False)
    batches: BatchedAnnexes = field(init=False, default_factory=BatchedAnnexes)
    to_download: List[ToDownload] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.digester = Digester(digests=["sha256"])
        # self.cache = PersistentCache("backups2datalad")
        # self.cache.clear()
        self.local_assets = set(dataset_files(self.ds.pathobj))
        try:
            with (self.ds.pathobj / ".dandi" / "assets.json").open() as fp:
                for md in json.load(fp):
                    if isinstance(md, str):
                        # Old version of assets.json; ignore
                        pass
                    else:
                        self.saved_metadata[md["path"].lstrip("/")] = md
        except FileNotFoundError:
            pass

    def __enter__(self) -> "Syncer":
        return self

    def __exit__(self, *_exc_info: Any) -> None:
        self.batches.close()

    def mkkey(self, filename: str, size: int, sha256_digest: str) -> str:
        return cast(
            str,
            self.batches.get(
                "examinekey",
                annex_options=["--migrate-to-backend=SHA256E"],
                path=self.ds.path,
            )(f"SHA256-s{size}--{sha256_digest} {filename}"),
        )

    def get_key_remotes(self, key: str) -> Optional[List[str]]:
        # Returns None if key is not known to git-annex
        whereis = self.batches.get(
            "whereis", annex_options=["--batch-keys"], path=self.ds.path, json=True
        )(key)
        if whereis["success"]:
            return [
                w["description"].strip("[]")
                for w in whereis["whereis"] + whereis["untrusted"]
            ]
        else:
            return None

    # do not cache due to fscacher resolving the path so we end
    # up with a path under .git/annex/objects
    # https://github.com/con/fscacher/issues/44
    # @cache.memoize_path
    def get_annex_hash(self, filepath: Union[str, Path]) -> str:
        # OPT: do not bother checking or talking to annex --
        # shaves off about 20% of runtime on 000003, so let's just
        # not bother checking etc but judge from the resolved path to be
        # under (some) annex
        realpath = os.path.realpath(filepath)
        if os.path.islink(filepath) and ".git/annex/object" in realpath:
            return os.path.basename(realpath).split("-")[-1].partition(".")[0]
        else:
            log.debug("Asset not under annex; calculating sha256 digest ourselves")
            return cast(str, self.digester(filepath)["sha256"])

    def sync_assets(self, assetiter: Iterable[RemoteAsset]) -> None:
        now = datetime.now(timezone.utc)
        downloading = True
        for asset in sorted(assetiter, key=attrgetter("created")):
            if downloading:
                try:
                    sha256_digest = asset.get_digest(DigestType.sha2_256)
                except NotFoundError:
                    log.info(
                        "SHA256 for %s has not been computed yet;"
                        " not downloading any more assets",
                        asset.path,
                    )
                    if now - asset.created > timedelta(days=1):
                        raise RuntimeError(
                            f"Asset {asset.path} created more than a day ago"
                            " but SHA256 digest has not yet been computed"
                        )
                    downloading = False
                    self.future_assets.add(asset.path)
                else:
                    self.process_asset(asset, sha256_digest)
            else:
                log.info("Will download %s in a future run", asset.path)
                self.future_assets.add(asset.path)
        self.download_assets()

    def process_asset(self, asset: RemoteAsset, sha256_digest: str) -> None:
        dest = self.ds.pathobj / asset.path
        self.local_assets.discard(asset.path)
        adict = asset2dict(asset)
        self.asset_metadata.append(adict)
        if self.config.force != "check" and adict == self.saved_metadata.get(
            asset.path
        ):
            log.debug(
                "Asset %s metadata unchanged; not taking any further action",
                asset.path,
            )
            return
        if not self.config.match_asset(asset.path):
            log.debug("Skipping asset %s", asset.path)
            return
        log.info("Syncing asset %s", asset.path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        to_update = False
        if not (dest.exists() or dest.is_symlink()):
            log.info("Asset not in dataset; will add")
            to_update = True
            self.added += 1
        else:
            log.debug("About to fetch hash from annex")
            if sha256_digest == self.get_annex_hash(dest):
                log.info(
                    "Asset in dataset, and hash shows no modification;"
                    " will not update"
                )
            else:
                log.info("Asset in dataset, and hash shows modification; will update")
                to_update = True
                self.updated += 1
        if to_update:
            bucket_url = self.config.get_file_bucket_url(asset)
            dest.unlink(missing_ok=True)
            key = self.mkkey(PurePosixPath(asset.path).name, asset.size, sha256_digest)
            remotes = self.get_key_remotes(key)
            if remotes is not None:
                log.info("Key is known to git-annex")
                self.ds.repo.call_annex(["fromkey", "--force", key, asset.path])
                self.register_url(asset.path, bucket_url)
                self.register_url(asset.path, asset.base_download_url)
                if (
                    self.config.backup_remote is not None
                    and self.config.backup_remote not in remotes
                ):
                    log.warn(
                        "Asset %s is not in backup remote %s",
                        asset.path,
                        self.config.backup_remote,
                    )
            else:
                log.info(
                    "Scheduling asset %s for download from %s", asset.path, bucket_url
                )
                self.to_download.append(
                    ToDownload(
                        path=asset.path,
                        url=bucket_url,
                        extra_urls=[asset.base_download_url],
                        sha256_digest=sha256_digest,
                    )
                )

    def download_assets(self) -> None:
        if not self.to_download:
            log.info("No assets scheduled for download")
            return
        log.info("Downloading %s", quantify(len(self.to_download), "scheduled asset"))
        urls_paths = [(td.url, td.path) for td in self.to_download]
        asyncio.run(download_urls(self.ds.pathobj, urls_paths, jobs=self.config.jobs))
        log.info("Finished downloading all assets")
        for td in self.to_download:
            if self.ds.repo.is_under_annex(td.path, batch=True):
                for u in td.extra_urls:
                    self.register_url(td.path, u)
            else:
                log.info("File is not managed by git annex; not adding URLs")
            if td.sha256_digest != (
                annex_hash := self.get_annex_hash(self.ds.pathobj / td.path)
            ):
                raise RuntimeError(
                    f"Hash mismatch for {td.path}!"
                    f"  Dandiarchive reports {td.sha256_digest},"
                    f" datalad reports {annex_hash}"
                )

    def register_url(self, path: str, url: str) -> None:
        log.info("Adding URL %s to asset", url)
        # Use registerurl because add_url_to_file() doesn't support files with
        # multiple URLs
        key = self.ds.repo.get_file_key(path)
        self.batches.get("registerurl", path=self.ds.path, output_proc=lambda _: None)(
            f"{key} {url}"
        )

    def prune_deleted(self) -> None:
        for apath in self.local_assets:
            if not self.config.match_asset(apath):
                pass
            elif apath in self.future_assets:
                try:
                    self.asset_metadata.append(self.saved_metadata[apath])
                except KeyError:
                    pass
            else:
                log.info(
                    "Asset %s is in dataset but not in Dandiarchive; deleting", apath
                )
                self.ds.repo.remove([apath])
                self.deleted += 1

    def dump_asset_metadata(self) -> None:
        dump(self.asset_metadata, self.ds.pathobj / ".dandi" / "assets.json")

    def get_commit_message(self) -> str:
        msgparts = []
        if self.added:
            msgparts.append(f"{quantify(self.added, 'file')} added")
        if self.updated:
            msgparts.append(f"{quantify(self.updated, 'file')} updated")
        if self.deleted:
            msgparts.append(f"{quantify(self.deleted, 'file')} deleted")
        if self.future_assets:
            msgparts.append(
                f"{quantify(len(self.future_assets), 'file')} not yet downloaded"
            )
        if not msgparts:
            msgparts.append("only some metadata updates")
        return f"[backups2datalad] {', '.join(msgparts)}"
