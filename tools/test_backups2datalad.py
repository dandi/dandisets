from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import subprocess
from typing import Any, Dict, Iterator, List, Optional

from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import DandiAPIClient, Version
from dandi.upload import upload
from dandi.utils import yaml_load
from datalad.api import Dataset
from datalad.tests.utils import assert_repo_status, ok_file_under_git
import pytest

from backups2datalad import DEFAULT_BRANCH
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.util import Config, custom_commit_date, readcmd

log = logging.getLogger("test_backups2datalad")


@pytest.fixture(autouse=True)
def capture_all_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="backups2datalad")
    caplog.set_level(logging.DEBUG, logger="test_backups2datalad")


@pytest.fixture(scope="session")
def dandi_client() -> DandiAPIClient:
    api_token = os.environ["DANDI_API_KEY"]
    with DandiAPIClient.for_dandi_instance("dandi-staging", token=api_token) as client:
        yield client


@pytest.fixture()
def text_dandiset(
    dandi_client: DandiAPIClient, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Dict[str, Any]]:
    d = dandi_client.create_dandiset(
        "Text Dandiset",
        {
            "schemaKey": "Dandiset",
            "name": "Text Dandiset",
            "description": "A test text Dandiset",
            "contributor": [
                {
                    "schemaKey": "Person",
                    "name": "Wodder, John",
                    "roleName": ["dcite:Author", "dcite:ContactPerson"],
                }
            ],
            "license": ["spdx:CC0-1.0"],
            "manifestLocation": ["https://github.com/dandi/dandi-cli"],
        },
    )
    dandiset_id = d.identifier
    dspath = tmp_path_factory.mktemp("text_dandiset")
    (dspath / dandiset_metadata_file).write_text(f"identifier: '{dandiset_id}'\n")
    (dspath / "file.txt").write_text("This is test text.\n")
    (dspath / "subdir1").mkdir()
    (dspath / "subdir1" / "apple.txt").write_text("Apple\n")
    (dspath / "subdir2").mkdir()
    (dspath / "subdir2" / "banana.txt").write_text("Banana\n")
    (dspath / "subdir2" / "coconut.txt").write_text("Coconut\n")

    def upload_dandiset(paths: Optional[List[str]] = None, **kwargs: Any) -> None:
        upload(
            paths=paths or [],
            dandiset_path=dspath,
            dandi_instance="dandi-staging",
            devel_debug=True,
            allow_any_path=True,
            validation="skip",
            **kwargs,
        )

    upload_dandiset()
    yield {
        "client": dandi_client,
        "dspath": dspath,
        "dandiset": d,
        "dandiset_id": dandiset_id,
        "reupload": upload_dandiset,
    }

    for v in d.get_versions():
        if v.identifier != "draft":
            dandi_client.delete(f"{d.api_path}versions/{v.identifier}/")
    d.delete()


def test_1(text_dandiset: Dict[str, Any], tmp_path: Path) -> None:
    # TODO: move pre-setup into a fixture, e.g. local_setup1 or make code work without?
    target_path = tmp_path / "target"
    di = DandiDatasetter(
        dandi_client=text_dandiset["client"],
        target_path=target_path,
        config=Config(
            # gh_org=None,
            # re_filter=None,
            # backup_remote=None,
            # jobs=jobs,
            # force=force,
            content_url_regex=r".*/blobs/",
            s3bucket="dandi-api-staging-dandisets",
        ),
    )

    with pytest.raises(Exception):
        log.info("test_1: Testing sync of nonexistent Dandiset")
        di.update_from_backup(["999999"])
    assert not (target_path / "999999").exists()

    # Since we are using text_dandiset, that immediately creates us a dandiset
    # TODO: may be separate it out, so we could start "clean" and still work ok
    # clean run without dandisets is ok
    # ret = di.update_from_backup()
    # assert ret is None, "nothing is returned ATM, if added -- test should be extended"

    dandiset_id = text_dandiset["dandiset_id"]
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])

    ds = Dataset(
        target_path / text_dandiset["dandiset_id"]
    )  # but we should get the super-dataset?
    assert_repo_status(ds.path)  # that all is clean etc
    ok_file_under_git(ds.path, "file.txt")

    (text_dandiset["dspath"] / "new.txt").write_text("This is a new file.\n")
    log.info("test_1: Updating test dandiset on server")
    text_dandiset["reupload"]()
    assert_repo_status(ds.path)  # no side-effects somehow
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    assert (ds.pathobj / "new.txt").read_text() == "This is a new file.\n"

    def readgit(*args: str) -> str:
        return readcmd("git", *args, cwd=ds.path)

    def check_version_tag(v: Version) -> None:
        vid = v.identifier

        # Assert tag has correct timestamp
        tag_ts = readgit(
            "for-each-ref", "--format=%(creatordate:iso-strict)", f"refs/tags/{vid}"
        )
        assert tag_ts == v.created.isoformat(timespec="seconds")

        # Assert tag has correct committer
        tag_creator = readgit("for-each-ref", "--format=%(creator)", f"refs/tags/{vid}")
        assert tag_creator.startswith("DANDI User <info@dandiarchive.org>")

        # Assert tagged commit has correct timestamp
        cmd_ts = readgit("show", "-s", "--format=%aI", f"{vid}^{{commit}}")
        assert cmd_ts == v.created.isoformat(timespec="seconds")

        # Assert that tag was merged into default branch
        assert (
            subprocess.run(
                ["git", "merge-base", "--is-ancestor", vid, DEFAULT_BRANCH], cwd=ds.path
            ).returncode
            == 0
        )

        # Assert tag branches from default branch
        assert (
            subprocess.run(
                ["git", "merge-base", "--is-ancestor", f"{DEFAULT_BRANCH}^", vid],
                cwd=ds.path,
            ).returncode
            == 0
        )

        # Assert dandiset.yaml in tagged commit has doi
        metadata = yaml_load(readgit("show", f"{vid}:{dandiset_metadata_file}"))
        assert metadata.get("doi")

    log.info("test_1: Waiting for Dandiset to become valid")
    text_dandiset["dandiset"].wait_until_valid(65)
    log.info("test_1: Publishing Dandiset")
    v1 = text_dandiset["dandiset"].publish().version
    version1 = v1.identifier
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    tags = {t["name"]: t["hexsha"] for t in ds.repo.get_tags()}
    assert version1 in tags
    v1_hash = tags[version1]
    check_version_tag(v1)

    (text_dandiset["dspath"] / "new.txt").write_text(
        "This file's contents were changed.\n"
    )
    log.info("test_1: Updating test dandiset on server")
    text_dandiset["reupload"]()
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    assert (
        ds.pathobj / "new.txt"
    ).read_text() == "This file's contents were changed.\n"

    log.info("test_1: Waiting for Dandiset to become valid")
    text_dandiset["dandiset"].wait_until_valid(65)
    log.info("test_1: Publishing Dandiset")
    v2 = text_dandiset["dandiset"].publish().version
    version2 = v2.identifier
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    tags = {t["name"]: t["hexsha"] for t in ds.repo.get_tags()}
    assert version1 in tags
    assert tags[version1] == v1_hash
    assert version2 in tags
    check_version_tag(v2)

    commit_authors = readgit("log", "--no-merges", "--format=%an <%ae>").splitlines()
    assert commit_authors == ["DANDI User <info@dandiarchive.org>"] * len(
        commit_authors
    )


def test_2(text_dandiset: Dict[str, Any], tmp_path: Path) -> None:
    target_path = tmp_path / "target"
    di = DandiDatasetter(
        dandi_client=text_dandiset["client"],
        target_path=target_path,
        config=Config(
            content_url_regex=r".*/blobs/",
            s3bucket="dandi-api-staging-dandisets",
            enable_tags=False,
        ),
    )

    dandiset_id = text_dandiset["dandiset_id"]
    log.info("test_2: Creating new backup of Dandiset")
    di.update_from_backup([dandiset_id])
    ds = Dataset(target_path / text_dandiset["dandiset_id"])

    def readgit(*args: str) -> str:
        return readcmd("git", *args, cwd=ds.path)

    versions = []
    for i in range(1, 4):
        (text_dandiset["dspath"] / "counter.txt").write_text(f"{i}\n")
        text_dandiset["reupload"]()
        log.info("test_2: Publishing version #%s", i)
        text_dandiset["dandiset"].wait_until_valid(65)
        v = text_dandiset["dandiset"].publish().version
        log.info(
            "test_2: Updating backup (release-tagging disabled) for version #%s", i
        )
        di.update_from_backup([dandiset_id])
        assert readgit("tag") == ""
        assert (ds.pathobj / "counter.txt").read_text() == f"{i}\n"
        base = readgit("show", "-s", "--format=%H", "HEAD")
        versions.append((v, base))

    di.config.enable_tags = True
    log.info("test_2: Updating backup, now with release-tagging enabled")
    di.update_from_backup([dandiset_id])

    for v, base in versions:
        assert readgit("rev-parse", f"{v.identifier}^") == base


def test_custom_commit_date(tmp_path: Path) -> None:
    ds = Dataset(str(tmp_path))
    ds.create(cfg_proc="text2git")
    (tmp_path / "file.txt").write_text("This is test text.\n")
    with custom_commit_date(datetime(2021, 6, 1, 12, 34, 56, tzinfo=timezone.utc)):
        ds.save(message="Add a file")
    about = readcmd("git", "show", "-s", "--format=%aI%n%an%n%ae", "HEAD", cwd=tmp_path)
    assert about.splitlines() == [
        "2021-06-01T12:34:56+00:00",
        "DANDI User",
        "info@dandiarchive.org",
    ]
