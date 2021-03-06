#!/usr/bin/env python

__requires__ = [
    "boto3",
    "click == 7.*",
    "dandi >= 0.7.0",
    "datalad",
    "fscacher",
    "humanize",
    "PyGitHub == 1.*",
    "requests ~= 2.20",
]

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
from copy import deepcopy
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
from time import sleep
from urllib.parse import urlparse, urlunparse

import boto3
from botocore import UNSIGNED
from botocore.client import Config
import click
from dandi import girder
from dandi.consts import dandiset_metadata_file
from dandi.dandiarchive import navigate_url
from dandi.dandiset import Dandiset
from dandi.support.digests import Digester
from dandi.utils import get_instance
import datalad
from datalad.api import Dataset
from datalad.support.json_py import dump
from fscacher import PersistentCache
from github import Github
from humanize import naturalsize
import requests

log = logging.getLogger(Path(sys.argv[0]).name)


@click.command()
@click.option("--backup-remote", help="Name of the rclone remote to push to")
@click.option(
    "-e", "--exclude", help="Skip dandisets matching the given regex", metavar="REGEX"
)
@click.option("-f", "--force", type=click.Choice(["check"]))
@click.option(
    "--gh-org", help="GitHub organization to create repositories under", required=True,
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
@click.option("--pdb", is_flag=True, help="Drop into debugger if an error occurs")
@click.option(
    "--re-filter", help="Only consider assets matching the given regex", metavar="REGEX"
)
@click.option(
    "--update-github-metadata", is_flag=True, help="Only update repositories' metadata",
)
@click.argument("assetstore", type=click.Path(exists=True, file_okay=False))
@click.argument("target", type=click.Path(file_okay=False))
@click.argument("dandisets", nargs=-1)
def main(
    assetstore,
    target,
    dandisets,
    ignore_errors,
    gh_org,
    re_filter,
    backup_remote,
    jobs,
    force,
    update_github_metadata,
    pdb,
    exclude,
):
    if pdb:
        sys.excepthook = pdb_excepthook
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=logging.INFO,
        force=True,  # Override dandi's settings
    )
    di = DatasetInstantiator(
        assetstore_path=Path(assetstore),
        target_path=Path(target),
        ignore_errors=ignore_errors,
        gh_org=gh_org,
        re_filter=re_filter and re.compile(re_filter),
        backup_remote=backup_remote,
        jobs=jobs,
        force=force,
    )
    if update_github_metadata:
        di.update_github_metadata(dandisets, exclude)
    else:
        di.run(dandisets, exclude)


class DatasetInstantiator:
    def __init__(
        self,
        assetstore_path: Path,
        target_path: Path,
        gh_org,
        ignore_errors=False,
        re_filter=None,
        backup_remote=None,
        jobs=10,
        force=None,
    ):
        self.assetstore_path = assetstore_path
        self.target_path = target_path
        self.ignore_errors = ignore_errors
        self.gh_org = gh_org
        self.re_filter = re_filter
        self.backup_remote = backup_remote
        self.jobs = jobs
        self.force = force
        self.session = None
        self._dandi_client = None
        self._s3client = None
        self._gh = None

    def run(self, dandisets=(), exclude=None):
        if not (self.assetstore_path / "girder-assetstore").exists():
            raise RuntimeError(
                "Given assetstore path does not contain girder-assetstore folder"
            )
        self.target_path.mkdir(parents=True, exist_ok=True)
        datalad.cfg.set("datalad.repo.backend", "SHA256E", where="override")
        with requests.Session() as self.session:
            for did in dandisets or self.get_dandiset_ids():
                if exclude is not None and re.search(exclude, did):
                    log.debug("Skipping dandiset %s", did)
                    continue
                dsdir = self.target_path / did
                log.info("Syncing Dandiset %s", did)
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
                        ds.repo._run_annex_command(
                            "untrust", annex_options=[self.backup_remote]
                        )
                        ds.repo.set_preferred_content(
                            "wanted",
                            "(not metadata=distribution-restrictions=*)",
                            remote=self.backup_remote,
                        )

                if self.sync_dataset(did, ds):
                    if "github" not in ds.repo.get_remotes():
                        log.info("Creating GitHub sibling for %s", did)
                        ds.create_sibling_github(
                            reponame=did,
                            existing="skip",
                            name="github",
                            access_protocol="https",
                            github_organization=self.gh_org,
                            publish_depends=self.backup_remote,
                        )
                        ds.config.set(
                            "remote.github.pushurl",
                            f"git@github.com:{self.gh_org}/{did}.git",
                            where="local",
                        )
                        ds.config.set("branch.master.remote", "github", where="local")
                        ds.config.set(
                            "branch.master.merge", "refs/heads/master", where="local"
                        )
                        self.gh.get_repo(f"{self.gh_org}/{did}").edit(
                            homepage=f"https://identifiers.org/DANDI:{did}"
                        )
                    else:
                        log.debug("GitHub remote already exists for %s", did)
                    log.info("Pushing to sibling")
                    ds.push(to="github", jobs=self.jobs)
                    self.gh.get_repo(f"{self.gh_org}/{did}").edit(
                        description=self.describe_dandiset(did)
                    )

    def sync_dataset(self, dandiset_id, ds):
        # Returns true if any changes were committed to the repository
        if ds.repo.dirty:
            raise RuntimeError("Dirty repository; clean or save before running")
        digester = Digester(digests=["sha256"])
        cache = PersistentCache("backups2datalad")
        cache.clear()

        @cache.memoize_path
        def get_annex_hash(filepath):
            relpath = str(Path(filepath).relative_to(dsdir))
            if ds.repo.is_under_annex(relpath, batch=True):
                return ds.repo.get_file_key(relpath).split("-")[-1].partition(".")[0]
            else:
                return digester(filepath)["sha256"]

        dsdir = ds.pathobj
        latest_mtime = None
        added = 0
        updated = 0
        deleted = 0

        with dandi_logging(dsdir) as logfile, navigate_url(
            f"https://dandiarchive.org/dandiset/{dandiset_id}/draft"
        ) as (_, dandiset, assets):
            log.info("Updating metadata file")
            try:
                (dsdir / dandiset_metadata_file).unlink()
            except FileNotFoundError:
                pass
            metadata = dandiset.get("metadata", {})
            Dandiset(dsdir, allow_empty=True).update_metadata(metadata)
            ds.repo.add([dandiset_metadata_file])
            local_assets = set(dataset_files(dsdir))
            local_assets.discard(dsdir / dandiset_metadata_file)
            asset_metadata = []
            saved_metadata = {}
            try:
                with (dsdir / ".dandi" / "assets.json").open() as fp:
                    for md in json.load(fp):
                        if isinstance(md, str):
                            # Old version of assets.json; ignore
                            pass
                        else:
                            saved_metadata[md["path"]] = md
            except FileNotFoundError:
                pass
            for a in assets:
                dest = dsdir / a["path"].lstrip("/")
                deststr = str(dest.relative_to(dsdir))
                local_assets.discard(dest)
                am = deepcopy(a)
                am["modified"] = str(am["modified"])
                asset_metadata.append(am)
                if (
                    self.force is None or "check" not in self.force
                ) and am == saved_metadata.get(am["path"]):
                    log.debug(
                        "Asset %s metadata unchanged; not taking any further action",
                        a["path"],
                    )
                    continue
                if self.re_filter and not self.re_filter.search(a["path"]):
                    log.debug("Skipping asset %s", a["path"])
                    continue
                log.info("Syncing asset %s", a["path"])
                gid = a["girder"]["id"]
                dandi_hash = a.get("sha256")
                if dandi_hash is None:
                    log.warning("Asset metadata does not include sha256 hash")
                mtime = a["modified"]  # type: datetime
                bucket_url = self.get_file_bucket_url(gid)
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.exists():
                    log.info("Asset not in dataset; will copy")
                    to_update = True
                    added += 1
                elif dandi_hash is not None:
                    if dandi_hash == get_annex_hash(dest):
                        log.info(
                            "Asset in dataset, and hash shows no modification; will not update"
                        )
                        to_update = False
                    else:
                        log.info(
                            "Asset in dataset, and hash shows modification; will update"
                        )
                        to_update = True
                        updated += 1
                else:
                    sz = dest.stat().st_size
                    if sz == a["attrs"]["size"]:
                        log.info(
                            "Asset in dataset, hash not available,"
                            " and size is unchanged; will not update"
                        )
                        to_update = False
                    else:
                        raise RuntimeError(
                            f"Size mismatch for {dest.relative_to(self.target_path)}!"
                            f"  Dandiarchive reports {a['attrs']['size']},"
                            f" local asset is size {sz}"
                        )
                if to_update:
                    src = self.assetstore_path / urlparse(bucket_url).path.lstrip("/")
                    if src.exists():
                        try:
                            self.mklink(src, dest)
                        except Exception:
                            if self.ignore_errors:
                                log.warning("cp command failed; ignoring")
                                continue
                            else:
                                raise
                        log.info("Adding asset to dataset")
                        ds.repo.add([deststr])
                        if ds.repo.is_under_annex(deststr, batch=True):
                            log.info("Adding URL %s to asset", bucket_url)
                            ds.repo.add_url_to_file(deststr, bucket_url, batch=True)
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
                        ds.download_url(urls=bucket_url, path=deststr)
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
            for a in local_assets:
                astr = str(a.relative_to(dsdir))
                if self.re_filter and not self.re_filter.search(astr):
                    continue
                log.info(
                    "Asset %s is in dataset but not in Dandiarchive; deleting", astr
                )
                ds.repo.remove([astr])
                deleted += 1
            dump(asset_metadata, dsdir / ".dandi" / "assets.json")
        if any(r["state"] != "clean" for r in ds.status()):
            log.info("Commiting changes")
            msgbody = ""
            if added:
                msgbody += f"{added} files added\n"
            if updated:
                msgbody += f"{updated} files updated\n"
            if deleted:
                msgbody += f"{deleted} files deleted\n"
            with custom_commit_date(latest_mtime):
                ds.save(message=f"Ran backups2datalad.py\n\n{msgbody}")
            return True
        else:
            log.info("No changes made to repository; deleting logfile")
            logfile.unlink()
            return False

    def update_github_metadata(self, dandisets=(), exclude=None):
        for did in dandisets or self.get_dandiset_ids():
            if exclude is not None and re.search(exclude, did):
                log.debug("Skipping dandiset %s", did)
                continue
            log.info("Setting metadata for %s/%s ...", self.gh_org, did)
            self.gh.get_repo(f"{self.gh_org}/{did}").edit(
                homepage=f"https://identifiers.org/DANDI:{did}",
                description=self.describe_dandiset(did),
            )

    @property
    def dandi_client(self):
        if self._dandi_client is None:
            dandi_instance = get_instance("dandi")
            self._dandi_client = girder.get_client(
                dandi_instance.girder, authenticate=False
            )
        return self._dandi_client

    def get_dandiset_ids(self):
        for d in self.dandi_client.listResource("dandi"):
            yield d["meta"]["dandiset"]["identifier"]

    def describe_dandiset(self, dandiset_id):
        about = self.dandi_client.getResource("dandi", dandiset_id)["meta"]["dandiset"]
        desc = about["name"]
        contact = ", ".join(
            c["name"]
            for c in about.get("contributors", [])
            if "ContactPerson" in c.get("roles", []) and "name" in c
        )
        stats = self.dandi_client.getResource("dandi", dandiset_id, "stats")
        num_files = stats["items"]
        size = naturalsize(stats["bytes"])
        if contact:
            return f"{num_files} files, {size}, {contact}, {desc}"
        else:
            return f"{num_files} files, {size}, {desc}"

    def get_file_bucket_url(self, girder_id):
        while True:
            r = (self.session or requests).head(
                f"https://girder.dandiarchive.org/api/v1/file/{girder_id}/download"
            )
            if r.status_code >= 500:
                log.debug(
                    "girder.dandiarchive.org returned %d; retrying", r.status_code
                )
                sleep(0.5)
            else:
                r.raise_for_status()
                break
        urlbits = urlparse(r.headers["Location"])
        s3meta = self.s3client.get_object(
            Bucket="dandiarchive", Key=urlbits.path.lstrip("/")
        )
        return urlunparse(urlbits._replace(query=f"versionId={s3meta['VersionId']}"))

    @property
    def s3client(self):
        if self._s3client is None:
            self._s3client = boto3.client(
                "s3", config=Config(signature_version=UNSIGNED)
            )
        return self._s3client

    @property
    def gh(self):
        if self._gh is None:
            token = subprocess.check_output(
                ["git", "config", "hub.oauthtoken"], universal_newlines=True
            ).strip()
            self._gh = Github(token)
        return self._gh

    @staticmethod
    def mklink(src, dest):
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
def custom_commit_date(dt):
    if dt is not None:
        with envvar_set("GIT_AUTHOR_DATE", str(dt)):
            with envvar_set("GIT_COMMITTER_DATE", str(dt)):
                yield
    else:
        yield


@contextmanager
def envvar_set(name, value):
    oldvalue = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if oldvalue is not None:
            os.environ[name] = oldvalue
        else:
            del os.environ[name]


def dataset_files(dspath):
    files = deque(
        p
        for p in dspath.iterdir()
        if p.name not in (".dandi", ".datalad", ".git", ".gitattributes")
    )
    while files:
        p = files.popleft()
        if p.is_file():
            yield p
        elif p.is_dir():
            files.extend(p.iterdir())


@contextmanager
def dandi_logging(dandiset_path: Path):
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


def is_interactive():
    """Return True if all in/outs are tty"""
    return sys.stdin.isatty() and sys.stdout.isatty() and sys.stderr.isatty()


def pdb_excepthook(exc_type, exc_value, tb):
    import traceback

    traceback.print_exception(exc_type, exc_value, tb)
    print()
    if is_interactive():
        import pdb

        pdb.post_mortem(tb)


if __name__ == "__main__":
    main()
