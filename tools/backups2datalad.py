#!/usr/bin/env python

"""
IMPORTANT NOTE ABOUT GITHUB CREDENTIALS

This script interacts with the GitHub API (in order to create & configure
repositories), and so it requires a GitHub OAuth token stored in the global Git
config under `hub.oauthtoken`.  In addition, the script also pushes commits to
GitHub over SSH, and so an SSH key that has been registered with a GitHub
account is needed as well.

Maybe TODO:
    - do not push in here, push will be outside upon success of the entire hierarchy

Later TODOs

- become aware of superdataset, add new subdatasets if created and ran
  not for a specific subdataset
- parallelize across datasets or may be better files (would that be possible within
  dataset?) using DataLad's #5022 ConsumerProducer?

"""

from collections import deque
from contextlib import contextmanager
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
from types import TracebackType
from typing import Dict, Iterator, List, Optional, Sequence, Set, Type, Union
from urllib.parse import urlparse, urlunparse

import boto3
from botocore import UNSIGNED
from botocore.client import Config, S3
import click
from click_loglevel import LogLevel
from dandi.consts import dandiset_metadata_file, known_instances
from dandi.dandiapi import DandiAPIClient, RemoteAsset, RemoteDandiset
from dandi.dandiset import APIDandiset
from dandi.metadata import get_asset_metadata
from dandi.support.digests import Digester, get_digest
from dandi.utils import ensure_datetime, ensure_strtime
from dandischema import models
from dandischema.consts import DANDI_SCHEMA_VERSION
import datalad
from datalad.api import Dataset
from datalad.support.json_py import dump

# from fscacher import PersistentCache
from github import Github
from humanize import naturalsize
from morecontext import envset
from packaging.version import Version

if sys.version_info[:2] >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

log = logging.getLogger(Path(sys.argv[0]).name)


@click.group()
@click.option(
    "-i",
    "--dandi-instance",
    type=click.Choice(sorted(known_instances)),
    default="dandi",
    help="DANDI instance to use",
    show_default=True,
)
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default="INFO",
    help="Set logging level",
    show_default=True,
)
@click.option("--pdb", is_flag=True, help="Drop into debugger if an error occurs")
@click.pass_context
def main(ctx: click.Context, log_level: int, pdb: bool, dandi_instance: str):
    ctx.obj = ctx.with_resource(DandiAPIClient.for_dandi_instance(dandi_instance))
    if pdb:
        sys.excepthook = pdb_excepthook
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level,
        force=True,  # Override dandi's settings
    )


@main.command()
@click.option("--backup-remote", help="Name of the rclone remote to push to")
@click.option(
    "-e", "--exclude", help="Skip dandisets matching the given regex", metavar="REGEX"
)
@click.option("-f", "--force", type=click.Choice(["check"]))
@click.option(
    "--gh-org",
    help="GitHub organization to create repositories under",
    required=True,
)
@click.option("-i", "--ignore-errors", is_flag=True)
@click.option(
    "-J",
    "--jobs",
    type=int,
    default=10,
    help="How many parallel jobs to use when pushing",
    show_default=True,
)
@click.option(
    "--re-filter", help="Only consider assets matching the given regex", metavar="REGEX"
)
@click.option(
    "--update-github-metadata",
    is_flag=True,
    help="Only update repositories' metadata",
)
@click.option("--update-asset-meta-version", is_flag=True)
@click.argument(
    "assetstore", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.argument("target", type=click.Path(file_okay=False, path_type=Path))
@click.argument("dandisets", nargs=-1)
@click.pass_obj
def update_from_backup(
    client: DandiAPIClient,
    assetstore: Path,
    target: Path,
    dandisets: Sequence[str],
    ignore_errors: bool,
    gh_org: str,
    re_filter: str,
    backup_remote: Optional[str],
    jobs: int,
    force: Optional[str],
    update_github_metadata: bool,
    update_asset_meta_version: bool,
    exclude: Optional[str],
):
    di = DatasetInstantiator(
        dandi_client=client,
        assetstore_path=assetstore,
        target_path=target,
        ignore_errors=ignore_errors,
        gh_org=gh_org,
        re_filter=re_filter and re.compile(re_filter),
        backup_remote=backup_remote,
        jobs=jobs,
        force=force,
        update_asset_meta_version=update_asset_meta_version,
        exclude=exclude and re.compile(exclude),
    )
    if update_github_metadata:
        di.update_github_metadata(dandisets)
    else:
        di.run(dandisets)


@main.command()
@click.option("--commitish", metavar="COMMITISH")
@click.option(
    "-d",
    "--dandiset-path",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=Path(),
)
@click.argument("dandiset")
@click.argument("version")
@click.pass_obj
def release(
    client: DandiAPIClient,
    dandiset: str,
    version: str,
    dandiset_path: Path,
    commitish: Optional[str],
):
    ds = client.get_dandiset(dandiset, version)
    mkrelease(dandiset_path, ds, commitish=commitish)


class DatasetInstantiator:
    def __init__(
        self,
        dandi_client: DandiAPIClient,
        assetstore_path: Path,
        target_path: Path,
        gh_org: str,
        ignore_errors: bool = False,
        re_filter: Optional[re.Pattern] = None,
        backup_remote: Optional[str] = None,
        jobs: int = 10,
        force: Optional[str] = None,
        update_asset_meta_version: bool = False,
        exclude: Optional[re.Pattern] = None,
    ):
        self.dandi_client = dandi_client
        self.assetstore_path = assetstore_path
        self.target_path = target_path
        self.ignore_errors = ignore_errors
        self.gh_org = gh_org
        self.re_filter = re_filter
        self.backup_remote = backup_remote
        self.jobs = jobs
        self.force = force
        self.update_asset_meta_version = update_asset_meta_version
        self.exclude = exclude

    def run(self, dandiset_ids: Sequence[str]) -> None:
        if not (self.assetstore_path / "girder-assetstore").exists():
            raise RuntimeError(
                "Given assetstore path does not contain girder-assetstore folder"
            )
        self.target_path.mkdir(parents=True, exist_ok=True)
        datalad.cfg.set("datalad.repo.backend", "SHA256E", where="override")
        for d in self.get_dandisets(dandiset_ids):
            dsdir = self.target_path / d.identifier
            log.info("Syncing Dandiset %s", d.identifier)
            ds = Dataset(str(dsdir))
            if not ds.is_installed():
                log.info("Creating Datalad dataset")
                ds.create(cfg_proc="text2git")
                if self.backup_remote is not None:
                    ds.repo.init_remote(
                        self.backup_remote,
                        [
                            "type=external",
                            "externaltype=rclone",
                            "chunk=1GB",
                            f"target={self.backup_remote}",  # I made them matching
                            "prefix=dandi-dandisets/annexstore",
                            "embedcreds=no",
                            "uuid=727f466f-60c3-4778-90b2-b2332856c2f8",
                            "encryption=none",
                            # shared, initialized in 000003
                        ],
                    )
                    ds.repo.call_annex(["untrust", self.backup_remote])
                    ds.repo.set_preferred_content(
                        "wanted",
                        "(not metadata=distribution-restrictions=*)",
                        remote=self.backup_remote,
                    )

            if self.sync_dataset(d, ds):
                if "github" not in ds.repo.get_remotes():
                    log.info("Creating GitHub sibling for %s", d.identifier)
                    ds.create_sibling_github(
                        reponame=d.identifier,
                        existing="skip",
                        name="github",
                        access_protocol="https",
                        github_organization=self.gh_org,
                        publish_depends=self.backup_remote,
                    )
                    ds.config.set(
                        "remote.github.pushurl",
                        f"git@github.com:{self.gh_org}/{d.identifier}.git",
                        where="local",
                    )
                    ds.config.set("branch.master.remote", "github", where="local")
                    ds.config.set(
                        "branch.master.merge", "refs/heads/master", where="local"
                    )
                    self.gh.get_repo(f"{self.gh_org}/{d.identifier}").edit(
                        homepage=f"https://identifiers.org/DANDI:{d.identifier}"
                    )
                else:
                    log.debug("GitHub remote already exists for %s", d.identifier)
                log.info("Pushing to sibling")
                ds.push(to="github", jobs=self.jobs)
                self.gh.get_repo(f"{self.gh_org}/{d.identifier}").edit(
                    description=self.describe_dandiset(d)
                )

    def sync_dataset(self, dandiset: RemoteDandiset, ds: Dataset) -> bool:
        # Returns true if any changes were committed to the repository
        if ds.repo.dirty:
            raise RuntimeError("Dirty repository; clean or save before running")
        digester = Digester(digests=["sha256"])
        # cache = PersistentCache("backups2datalad")
        # cache.clear()

        # do not cache due to fscacher resolving the path so we end
        # up with a path under .git/annex/objects
        # https://github.com/con/fscacher/issues/44
        # @cache.memoize_path
        def get_annex_hash(filepath: Union[str, Path]) -> str:
            # relpath = str(Path(filepath).relative_to(dsdir))
            # OPT: do not bother checking or talking to annex --
            # shaves off about 20% of runtime on 000003, so let's just
            # not bother checking etc but judge from the resolved path to be
            # under (some) annex
            realpath = os.path.realpath(filepath)
            if (
                os.path.islink(filepath) and ".git/annex/object" in realpath
            ):  # ds.repo.is_under_annex(relpath, batch=True):
                return os.path.basename(realpath).split("-")[-1].partition(".")[0]
            else:
                log.debug("Asset not under annex; calculating sha256 digest ourselves")
                return digester(filepath)["sha256"]

        dsdir: Path = ds.pathobj
        latest_mtime: Optional[datetime] = None
        added = 0
        updated = 0
        deleted = 0

        with dandi_logging(dsdir) as logfile:
            if self.update_asset_meta_version:
                # we might need to update, so need to authenticate
                self.dandi_client.dandi_authenticate()
            log.info("Updating metadata file")
            try:
                (dsdir / dandiset_metadata_file).unlink()
            except FileNotFoundError:
                pass
            metadata = dandiset.get_raw_metadata()
            APIDandiset(dsdir, allow_empty=True).update_metadata(metadata)
            ds.repo.add([dandiset_metadata_file])
            local_assets: Set[str] = set(dataset_files(dsdir))
            local_assets.discard(dandiset_metadata_file)
            asset_metadata: List[dict] = []
            saved_metadata: Dict[str, dict] = {}
            try:
                with (dsdir / ".dandi" / "assets.json").open() as fp:
                    for md in json.load(fp):
                        if isinstance(md, str):
                            # Old version of assets.json; ignore
                            pass
                        else:
                            saved_metadata[md["path"].lstrip("/")] = md
            except FileNotFoundError:
                pass
            for a in dandiset.get_assets():
                dest = dsdir / a.path
                local_assets.discard(a.path)
                adict = {**a.json_dict(), "metadata": a.get_raw_metadata()}
                # Let's pop dandiset "linkage" since yoh thinks it should not be there
                # TODO/discussion: https://github.com/dandi/dandi-cli/issues/690
                for f in ("dandiset_id", "version_id"):
                    adict.pop(f, None)
                # and ensure that "modified" is a str
                adict["modified"] = ensure_strtime(adict["modified"])
                asset_metadata.append(adict)
                if (
                    self.force != "check"
                    and adict == saved_metadata.get(a.path)
                    and not self.update_asset_meta_version
                ):
                    log.debug(
                        "Asset %s metadata unchanged; not taking any further action",
                        a.path,
                    )
                    continue
                if self.re_filter and not self.re_filter.search(a.path):
                    log.debug("Skipping asset %s", a.path)
                    continue
                log.info("Syncing asset %s", a.path)
                try:
                    dandi_hash = adict["metadata"]["digest"]["dandi:sha2-256"]
                except KeyError:
                    dandi_hash = None
                    log.info(
                        "%s: %s: Asset metadata does not include sha256 hash",
                        dandiset.identifier,
                        a.path,
                    )
                dandi_etag = adict["metadata"]["digest"]["dandi:dandi-etag"]
                mtime = ensure_datetime(adict["metadata"]["dateModified"])
                download_url = a.download_url
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.exists():
                    log.info("Asset not in dataset; will copy")
                    to_update = True
                    added += 1
                elif dandi_hash is not None:
                    log.debug("About to fetch hash from annex")
                    if dandi_hash == get_annex_hash(dest):
                        log.info(
                            "Asset in dataset, and hash shows no modification;"
                            " will not update"
                        )
                        to_update = False
                    else:
                        log.info(
                            "Asset in dataset, and hash shows modification; will update"
                        )
                        to_update = True
                        updated += 1
                else:
                    log.debug("About to calculate dandi-etag")
                    if dandi_etag == get_digest(dest, "dandi-etag"):
                        log.info(
                            "Asset in dataset, and etag shows no modification;"
                            " will not update"
                        )
                        to_update = False
                    else:
                        log.info(
                            "Asset in dataset, and etag shows modification; will update"
                        )
                        to_update = True
                        updated += 1

                # Ad-hoc custom way to do asset metadata migration via
                # re-extraction.  This script should be run one dandiset at a
                # time.
                asset_schema_version = adict["metadata"].get("schemaVersion")
                if (
                    self.update_asset_meta_version
                    and not to_update
                    and asset_schema_version
                    and Version(asset_schema_version) < Version(DANDI_SCHEMA_VERSION)
                ):
                    log.info("Calculating new metadata for asset %s", a.path)
                    new_metadata = get_asset_metadata(
                        dest,
                        a.path,
                        digest=get_digest(dest, "dandi-etag"),
                        digest_type="dandi_etag",
                    )
                    if new_metadata.schemaVersion != DANDI_SCHEMA_VERSION:
                        raise RuntimeError(
                            "New metadata does not have expected schemaVersion!"
                            "  Expected %r, got %r."
                            % (DANDI_SCHEMA_VERSION, new_metadata.schemaVersion)
                        )
                    else:
                        log.info("Updating metadata for asset %s", a.path)
                        # we need to figure out blob_id since that is what API needs
                        # ATM. TODO: discussion is ongoing to change API
                        # see e.g. https://github.com/dandi/dandi-api/pull/382
                        blob_id_ret = a.client.post(
                            "/blobs/digest/",
                            json={
                                "algorithm": "dandi:dandi-etag",
                                "value": new_metadata.digest[
                                    models.DigestType.dandi_etag
                                ],
                            },
                        )
                        # no such API interface yet!
                        # a.set_metadata(new_metadata)
                        ret = a.client.put(
                            a.api_path,
                            json={
                                "metadata": new_metadata.json_dict(),
                                "blob_id": blob_id_ret["blob_id"],
                            },
                        )
                        assert adict["asset_id"] != ret["asset_id"]
                        assert adict["path"] == ret["path"]
                        # Update adict in-place so that the instance appended
                        # to asset_metadata gets updated:
                        adict.clear()
                        adict.update(ret)

                if to_update:
                    bucket_url = self.get_file_bucket_url(a)
                    src = self.assetstore_path / urlparse(bucket_url).path.lstrip("/")
                    if src.exists():
                        try:
                            self.mklink(src, dest)
                        except Exception:
                            if self.ignore_errors:
                                log.warning(
                                    "%s: %s: cp command failed; ignoring",
                                    dandiset.identifier,
                                    a.path,
                                )
                                continue
                            else:
                                raise
                        log.info("Adding asset to dataset")
                        ds.repo.add([a.path])
                        if ds.repo.is_under_annex(a.path, batch=True):
                            log.info("Adding URL %s to asset", bucket_url)
                            ds.repo.add_url_to_file(a.path, bucket_url, batch=True)
                            log.info("Adding URL %s to asset", download_url)
                            ds.repo.call_annex(
                                [
                                    "registerurl",
                                    ds.repo.get_file_key(a.path),
                                    download_url,
                                ]
                            )
                        else:
                            log.info("File is not managed by git annex; not adding URL")
                    else:
                        log.info(
                            "Asset not found in assetstore; downloading from %s",
                            bucket_url,
                        )
                        try:
                            dest.unlink()
                        except FileNotFoundError:
                            pass
                        ds.download_url(urls=bucket_url, path=a.path)
                        if ds.repo.is_under_annex(a.path, batch=True):
                            log.info("Adding URL %s to asset", download_url)
                            ds.repo.call_annex(
                                [
                                    "registerurl",
                                    ds.repo.get_file_key(a.path),
                                    download_url,
                                ]
                            )
                        else:
                            log.info("File is not managed by git annex; not adding URL")
                    if latest_mtime is None or mtime > latest_mtime:
                        latest_mtime = mtime
                if dandi_hash is not None:
                    annex_key = get_annex_hash(dest)
                    if dandi_hash != annex_key:
                        raise RuntimeError(
                            f"Hash mismatch for {dest.relative_to(self.target_path)}!"
                            f"  Dandiarchive reports {dandi_hash},"
                            f" datalad reports {annex_key}"
                        )
                else:
                    annex_etag = get_digest(dest, "dandi-etag")
                    if dandi_etag != annex_etag:
                        raise RuntimeError(
                            f"ETag mismatch for {dest.relative_to(self.target_path)}!"
                            f"  Dandiarchive reports {dandi_hash},"
                            f" file has {annex_etag}"
                        )
            for apath in local_assets:
                if self.re_filter and not self.re_filter.search(apath):
                    continue
                log.info(
                    "Asset %s is in dataset but not in Dandiarchive; deleting", apath
                )
                ds.repo.remove([apath])
                deleted += 1

            # due to  https://github.com/dandi/dandi-api/issues/231
            # we need to sanitize temporary URLs. TODO: remove when "fixed"
            for asset in asset_metadata:
                if "contentUrl" in asset["metadata"]:
                    asset["metadata"]["contentUrl"] = sanitize_contentUrls(
                        asset["metadata"]["contentUrl"]
                    )
            dump(asset_metadata, dsdir / ".dandi" / "assets.json")
        if any(r["state"] != "clean" for r in ds.status()):
            log.info("Commiting changes")
            msgparts = []
            if added:
                msgparts.append(f"{added} files added")
            if updated:
                msgparts.append(f"{updated} files updated")
            if deleted:
                msgparts.append(f"{deleted} files deleted")
            if not msgparts:
                msgparts.append("only some metadata updates")
            last_commit_time = ensure_datetime(
                subprocess.check_output(
                    ["git", "--no-pager", "show", "-s", "--format=%aI", "HEAD"],
                    universal_newlines=True,
                ).strip()
            )
            if latest_mtime is not None and last_commit_time > latest_mtime:
                latest_mtime = last_commit_time
            with custom_commit_date(latest_mtime):
                ds.save(message=f"[backups2datalad] {', '.join(msgparts)}")
            return True
        else:
            log.info("No changes made to repository; deleting logfile")
            logfile.unlink()
            return False

    def update_github_metadata(self, dandiset_ids: Sequence[str]) -> None:
        for d in self.get_dandisets(dandiset_ids):
            log.info("Setting metadata for %s/%s ...", self.gh_org, d.identifier)
            self.gh.get_repo(f"{self.gh_org}/{d.identifier}").edit(
                homepage=f"https://identifiers.org/DANDI:{d.identifier}",
                description=self.describe_dandiset(d),
            )

    def get_dandisets(self, dandiset_ids: Sequence[str]) -> Iterator[RemoteDandiset]:
        if dandiset_ids:
            diter = (
                self.dandi_client.get_dandiset(did, "draft") for did in dandiset_ids
            )
        else:
            diter = self.dandi_client.get_dandisets()
        for d in diter:
            if self.exclude is not None and self.exclude.search(d.identifier):
                log.debug("Skipping dandiset %s", d.identifier)
            else:
                yield d

    def describe_dandiset(self, dandiset: RemoteDandiset) -> str:
        metadata = dandiset.get_raw_metadata()
        desc = dandiset.version.name
        contact = ", ".join(
            c["name"]
            for c in metadata.get("contributor", [])
            if "dandi:ContactPerson" in c.get("roleName", []) and "name" in c
        )
        num_files = dandiset.version.asset_count
        size = naturalsize(dandiset.version.size)
        if contact:
            return f"{num_files} files, {size}, {contact}, {desc}"
        else:
            return f"{num_files} files, {size}, {desc}"

    def get_file_bucket_url(self, asset: RemoteAsset) -> str:
        log.debug("Fetching bucket URL for asset")
        aws_url = asset.get_content_url(r"amazonaws.com/.*blobs/")
        urlbits = urlparse(aws_url)
        log.debug("About to query S3")
        s3meta = self.s3client.get_object(
            Bucket="dandiarchive", Key=urlbits.path.lstrip("/")
        )
        log.debug("Got bucket URL for asset")
        return urlunparse(urlbits._replace(query=f"versionId={s3meta['VersionId']}"))

    @cached_property
    def s3client(self) -> S3:
        return boto3.client("s3", config=Config(signature_version=UNSIGNED))

    @cached_property
    def gh(self) -> Github:
        token = subprocess.check_output(
            ["git", "config", "hub.oauthtoken"], universal_newlines=True
        ).strip()
        return Github(token)

    @staticmethod
    def mklink(src: Union[str, Path], dest: Union[str, Path]) -> None:
        log.info("cp %s -> %s", src, dest)
        subprocess.run(
            [
                "cp",
                "-L",
                "--reflink=always",
                "--remove-destination",
                str(src),
                str(dest),
            ],
            check=True,
        )


@contextmanager
def custom_commit_date(dt: datetime) -> Iterator[None]:
    if dt is not None:
        with envset("GIT_AUTHOR_DATE", str(dt)):
            with envset("GIT_COMMITTER_DATE", str(dt)):
                yield
    else:
        yield


def dataset_files(dspath: Path) -> Iterator[str]:
    files = deque(
        p
        for p in dspath.iterdir()
        if p.name not in (".dandi", ".datalad", ".git", ".gitattributes")
    )
    while files:
        p = files.popleft()
        if p.is_file():
            yield str(p.relative_to(dspath))
        elif p.is_dir():
            files.extend(p.iterdir())


@contextmanager
def dandi_logging(dandiset_path: Path) -> Iterator[Path]:
    logdir = dandiset_path / ".git" / "dandi" / "logs"
    logdir.mkdir(exist_ok=True, parents=True)
    filename = "sync-{:%Y%m%d%H%M%SZ}-{}.log".format(datetime.utcnow(), os.getpid())
    logfile = logdir / filename
    handler = logging.FileHandler(logfile, encoding="utf-8")
    fmter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handler.setFormatter(fmter)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        yield logfile
    except Exception:
        log.exception("Operation failed with exception:")
        raise
    finally:
        root.removeHandler(handler)


def is_interactive() -> bool:
    """Return True if all in/outs are tty"""
    return sys.stdin.isatty() and sys.stdout.isatty() and sys.stderr.isatty()


def pdb_excepthook(
    exc_type: Type[BaseException], exc_value: BaseException, tb: TracebackType
) -> None:
    import traceback

    traceback.print_exception(exc_type, exc_value, tb)
    print()
    if is_interactive():
        import pdb

        pdb.post_mortem(tb)


def sanitize_contentUrls(urls: List[str]) -> List[str]:
    urls_ = []
    for url in urls:
        if "x-amz-expires=" in url.lower():
            # just strip away everything after ?
            url = url[: url.index("?")]
        urls_.append(url)
    return urls_


def mkrelease(repo: Path, dandiset: RemoteDandiset, commitish: str = None) -> None:
    # `dandiset` must have its version set to the published version
    if commitish is None:
        remote_assets = list(dandiset.get_assets())
        repo_assets = json.loads(
            (repo / ".dandi" / "assets.json").read_text(encoding="utf-8")
        )
        if assets_eq(remote_assets, repo_assets):
            commitish = "HEAD"
        else:
            for cmt in subprocess.check_output(
                ["git", "rev-list", "--skip=1", "HEAD", "--", ".dandi/assets.json"],
                cwd=repo,
                universal_newlines=True,
            ).splitlines():
                repo_assets = json.loads(
                    subprocess.check_output(
                        ["git", "show", f"{cmt}:.dandi/assets.json"],
                        cwd=repo,
                        universal_newlines=True,
                    )
                )
                if (
                    repo_assets
                    and not isinstance(repo_assets[0], dict)
                    or "asset_id" not in repo_assets[0]
                ):
                    # We've reached an older, pre-dandi-api version of
                    # assets.json; we're not going to find the assets we're
                    # after here
                    break
                if assets_eq(remote_assets, repo_assets):
                    commitish = cmt
                    break
            if commitish is None:
                raise RuntimeError(
                    "Could not find commit matching Dandiset "
                    f"{dandiset.identifier}, version {dandiset.version_id}"
                )
    subprocess.run(
        [
            "git",
            "tag",
            "-m",
            f"Version {dandiset.version_id} of Dandiset {dandiset.identifier}",
            dandiset.version_id,
            commitish,
        ],
        cwd=repo,
        check=True,
    )
    subprocess.run(["git", "push", "--follow-tags"], cwd=repo, check=True)


def assets_eq(remote_assets: List[RemoteAsset], local_assets: List[dict]) -> bool:
    return {a.identifier for a in remote_assets} == {
        a["asset_id"] for a in local_assets
    }


if __name__ == "__main__":
    main()
