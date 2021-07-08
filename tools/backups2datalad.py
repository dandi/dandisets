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
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
import json
import logging
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
from types import TracebackType
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Type, Union, cast
from urllib.parse import urlparse, urlunparse

import boto3
from botocore import UNSIGNED
from botocore.client import Config
import click
from click_loglevel import LogLevel
from dandi.consts import dandiset_metadata_file, known_instances
from dandi.dandiapi import DandiAPIClient, RemoteAsset, RemoteDandiset
from dandi.dandiset import APIDandiset
from dandi.support.digests import Digester, get_digest
from dandi.utils import ensure_datetime
import datalad
from datalad.api import Dataset
from datalad.support.json_py import dump
from github import Github
from humanize import naturalsize
from morecontext import envset
from mypy_boto3_s3 import S3Client

# from fscacher import PersistentCache

log = logging.getLogger(Path(sys.argv[0]).name)


@dataclass
class DandiDatasetter:
    dandi_client: DandiAPIClient
    assetstore_path: Path
    target_path: Path
    ignore_errors: bool = False
    asset_filter: Optional[re.Pattern] = None
    jobs: int = 10
    force: Optional[str] = None

    def update_from_backup(
        self,
        dandiset_ids: Sequence[str],
        exclude: Optional[re.Pattern],
        gh_org: Optional[str],
        backup_remote: Optional[str],
    ) -> None:
        if not (self.assetstore_path / "girder-assetstore").exists():
            raise RuntimeError(
                "Given assetstore path does not contain girder-assetstore folder"
            )
        self.target_path.mkdir(parents=True, exist_ok=True)
        datalad.cfg.set("datalad.repo.backend", "SHA256E", where="override")
        for d in self.get_dandisets(dandiset_ids, exclude=exclude):
            dsdir = self.target_path / d.identifier
            ds = self.init_dataset(dsdir, backup_remote=backup_remote)
            if self.sync_dataset(d, ds):
                if gh_org is not None:
                    self.ensure_github_remote(
                        ds, d.identifier, gh_org=gh_org, backup_remote=backup_remote
                    )
                self.tag_releases(d, ds, push=gh_org is not None)
                if gh_org is not None:
                    log.info("Pushing to sibling")
                    ds.push(to="github", jobs=self.jobs)
                    self.gh.get_repo(f"{gh_org}/{d.identifier}").edit(
                        description=self.describe_dandiset(d)
                    )

    def init_dataset(self, dsdir: Path, backup_remote: Optional[str]) -> Dataset:
        ds = Dataset(str(dsdir))
        if not ds.is_installed():
            log.info("Creating Datalad dataset")
            ds.create(cfg_proc="text2git")
            if backup_remote is not None:
                ds.repo.init_remote(
                    backup_remote,
                    [
                        "type=external",
                        "externaltype=rclone",
                        "chunk=1GB",
                        f"target={backup_remote}",  # I made them matching
                        "prefix=dandi-dandisets/annexstore",
                        "embedcreds=no",
                        "uuid=727f466f-60c3-4778-90b2-b2332856c2f8",
                        "encryption=none",
                        # shared, initialized in 000003
                    ],
                )
                ds.repo.call_annex(["untrust", backup_remote])
                ds.repo.set_preferred_content(
                    "wanted",
                    "(not metadata=distribution-restrictions=*)",
                    remote=backup_remote,
                )
        return ds

    def ensure_github_remote(
        self, ds: Dataset, dandiset_id: str, gh_org: str, backup_remote: Optional[str]
    ) -> None:
        if "github" not in ds.repo.get_remotes():
            log.info("Creating GitHub sibling for %s", dandiset_id)
            ds.create_sibling_github(
                reponame=dandiset_id,
                existing="skip",
                name="github",
                access_protocol="https",
                github_organization=gh_org,
                publish_depends=backup_remote,
            )
            ds.config.set(
                "remote.github.pushurl",
                f"git@github.com:{gh_org}/{dandiset_id}.git",
                where="local",
            )
            ds.config.set("branch.master.remote", "github", where="local")
            ds.config.set("branch.master.merge", "refs/heads/master", where="local")
            self.gh.get_repo(f"{gh_org}/{dandiset_id}").edit(
                homepage=f"https://identifiers.org/DANDI:{dandiset_id}"
            )
        else:
            log.debug("GitHub remote already exists for %s", dandiset_id)

    def sync_dataset(self, dandiset: RemoteDandiset, ds: Dataset) -> bool:
        # Returns true if any changes were committed to the repository
        log.info("Syncing Dandiset %s", dandiset.identifier)
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
                return cast(str, digester(filepath)["sha256"])

        dsdir: Path = ds.pathobj
        latest_mtime: Optional[datetime] = None
        added = 0
        updated = 0
        deleted = 0

        with dandi_logging(dsdir) as logfile:
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
                asset_metadata.append(adict)
                if self.force != "check" and adict == saved_metadata.get(a.path):
                    log.debug(
                        "Asset %s metadata unchanged; not taking any further action",
                        a.path,
                    )
                    continue
                if self.asset_filter and not self.asset_filter.search(a.path):
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
                if to_update:
                    bucket_url = self.get_file_bucket_url(a)
                    src = self.assetstore_path / urlparse(bucket_url).path.lstrip("/")
                    if src.exists():
                        try:
                            mklink(src, dest)
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
                if self.asset_filter and not self.asset_filter.search(apath):
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
                readcmd(
                    "git",
                    "--no-pager",
                    "show",
                    "-s",
                    "--format=%aI",
                    "HEAD",
                    cwd=dsdir,
                )
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

    def update_github_metadata(
        self, dandiset_ids: Sequence[str], gh_org: str, exclude: Optional[re.Pattern]
    ) -> None:
        for d in self.get_dandisets(dandiset_ids, exclude=exclude):
            log.info("Setting metadata for %s/%s ...", gh_org, d.identifier)
            self.gh.get_repo(f"{gh_org}/{d.identifier}").edit(
                homepage=f"https://identifiers.org/DANDI:{d.identifier}",
                description=self.describe_dandiset(d),
            )

    def get_dandisets(
        self, dandiset_ids: Sequence[str], exclude: Optional[re.Pattern]
    ) -> Iterator[RemoteDandiset]:
        if dandiset_ids:
            diter = (
                self.dandi_client.get_dandiset(did, "draft") for did in dandiset_ids
            )
        else:
            diter = self.dandi_client.get_dandisets()
        for d in diter:
            if exclude is not None and exclude.search(d.identifier):
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

    def tag_releases(self, dandiset: RemoteDandiset, ds: Dataset, push: bool) -> None:
        log.info("Tagging releases for Dandiset %s", dandiset.identifier)
        for v in dandiset.get_versions():
            if readcmd("git", "tag", "-l", v.identifier):
                log.debug("Version %s already tagged", v.identifier)
            else:
                log.info("Tagging version %s", v.identifier)
                self.mkrelease(dandiset.for_version(v), ds, push=push)

    def mkrelease(
        self,
        dandiset: RemoteDandiset,
        ds: Dataset,
        push: bool,
        commitish: Optional[str] = None,
    ) -> None:
        # `dandiset` must have its version set to the published version
        def git(*args: str, **kwargs: Any) -> None:
            log.debug("Running: git %s", " ".join(shlex.quote(str(a)) for a in args))
            subprocess.run(["git", *args], cwd=repo, check=True, **kwargs)

        repo: Path = ds.pathobj
        if commitish is None:
            commitish = readcmd(
                "git",
                "rev-list",
                f"--before={dandiset.version.created}",
                "-n1",
                "HEAD",
                cwd=repo,
            )
        remote_assets = list(dandiset.get_assets())
        repo_assets = json.loads(
            readcmd("git", "show", f"{commitish}:.dandi/assets.json", cwd=repo)
        )
        if (
            repo_assets
            and (
                not isinstance(repo_assets[0], dict) or "asset_id" not in repo_assets[0]
            )
        ) or not assets_eq(remote_assets, repo_assets):
            log.info(
                "Assets in commit %s do not match assets in version %s;"
                " creating branch for version",
                commitish,
                dandiset.version_id,
            )
            branching = True
            git("checkout", "-b", f"release-{dandiset.version_id}")
            self.sync_dataset(dandiset, ds)
        else:
            branching = False
        git(
            "tag",
            "-m",
            f"Version {dandiset.version_id} of Dandiset {dandiset.identifier}",
            dandiset.version_id,
            commitish,
        )
        if push:
            # if branching:
            #     git("push", "-u", "github", "release-{dandiset.version_id}")
            # git("push", "--follow-tags", "github")
            ds.push(to="github", jobs=self.jobs)
        if branching:
            git("checkout", "master")

    @cached_property
    def s3client(self) -> S3Client:
        return boto3.client("s3", config=Config(signature_version=UNSIGNED))

    @cached_property
    def gh(self) -> Github:
        token = readcmd("git", "config", "hub.oauthtoken")
        return Github(token)


@click.group()
@click.option(
    "--asset-filter",
    help="Only consider assets matching the given regex",
    metavar="REGEX",
)
@click.option(
    "-A",
    "--assetstore",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "-i",
    "--dandi-instance",
    type=click.Choice(sorted(known_instances)),
    default="dandi",
    help="DANDI instance to use",
    show_default=True,
)
@click.option(
    "-J",
    "--jobs",
    type=int,
    default=10,
    help="How many parallel jobs to use when pushing",
    show_default=True,
)
@click.option("-f", "--force", type=click.Choice(["check"]))
@click.option("--ignore-errors", is_flag=True)
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default="INFO",
    help="Set logging level  [default: INFO]",
)
@click.option("--pdb", is_flag=True, help="Drop into debugger if an error occurs")
@click.option(
    "-T", "--target", type=click.Path(file_okay=False, path_type=Path), required=True
)
@click.pass_context
def main(
    ctx: click.Context,
    asset_filter: str,
    assetstore: Path,
    dandi_instance: str,
    force: Optional[str],
    ignore_errors: bool,
    jobs: int,
    log_level: int,
    pdb: bool,
    target: Path,
) -> None:
    ctx.obj = DandiDatasetter(
        dandi_client=ctx.with_resource(
            DandiAPIClient.for_dandi_instance(dandi_instance)
        ),
        assetstore_path=assetstore,
        target_path=target,
        ignore_errors=ignore_errors,
        asset_filter=re.compile(asset_filter) if asset_filter is not None else None,
        jobs=jobs,
        force=force,
    )
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
@click.option("--gh-org", help="GitHub organization to create repositories under")
@click.argument("dandisets", nargs=-1)
@click.pass_obj
def update_from_backup(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    backup_remote: Optional[str],
    gh_org: Optional[str],
    exclude: Optional[str],
) -> None:
    exclude_rgx = re.compile(exclude) if exclude is not None else None
    datasetter.update_from_backup(
        dandisets, exclude=exclude_rgx, gh_org=gh_org, backup_remote=backup_remote
    )


@main.command()
@click.option(
    "-e", "--exclude", help="Skip dandisets matching the given regex", metavar="REGEX"
)
@click.option(
    "--gh-org",
    help="GitHub organization under which repositories reside",
    required=True,
)
@click.argument("dandisets", nargs=-1)
@click.pass_obj
def update_github_metadata(
    datasetter: DandiDatasetter,
    dandisets: Sequence[str],
    exclude: Optional[str],
    gh_org: str,
) -> None:
    exclude_rgx = re.compile(exclude) if exclude is not None else None
    datasetter.update_github_metadata(dandisets, exclude=exclude_rgx, gh_org=gh_org)


@main.command()
@click.option("--commitish", metavar="COMMITISH")
@click.option("--push/--no-push", default=True)
@click.argument("dandiset")
@click.argument("version")
@click.pass_obj
def release(
    datasetter: DandiDatasetter,
    dandiset: str,
    version: str,
    commitish: Optional[str],
    push: bool,
) -> None:
    dandiset_obj = datasetter.dandi_client.get_dandiset(dandiset, version)
    dataset = Dataset(str(datasetter.target_path / dandiset))
    datasetter.mkrelease(dandiset_obj, dataset, commitish=commitish, push=push)


@contextmanager
def custom_commit_date(dt: Optional[datetime]) -> Iterator[None]:
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


def assets_eq(remote_assets: List[RemoteAsset], local_assets: List[dict]) -> bool:
    return {a.identifier for a in remote_assets} == {
        a["asset_id"] for a in local_assets
    }


def readcmd(*args: Union[str, Path], **kwargs: Any) -> str:
    log.debug("Running: %s", shlex.join(map(str, args)))
    return cast(str, subprocess.check_output(args, text=True, **kwargs)).strip()


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


if __name__ == "__main__":
    main()
