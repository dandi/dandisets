from datetime import datetime, timezone
import logging
from pathlib import Path
from time import sleep
from traceback import format_exception

from click.testing import CliRunner, Result
from conftest import SampleDandiset
from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import Version
from dandi.utils import find_files, yaml_load
from datalad.api import Dataset
from datalad.support.annexrepo import AnnexRepo
from datalad.tests.utils import assert_repo_status, ok_file_under_git
import pytest
from test_util import GitRepo

from backups2datalad.__main__ import main
from backups2datalad.consts import DEFAULT_BRANCH
from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.util import Config, custom_commit_date, is_meta_file

log = logging.getLogger("test_backups2datalad.test_core")


def show_result(r: Result) -> str:
    if r.exception is not None:
        assert isinstance(r.exc_info, tuple)
        return "".join(format_exception(*r.exc_info))
    else:
        return r.output


def test_1(text_dandiset: SampleDandiset, tmp_path: Path) -> None:
    # TODO: move pre-setup into a fixture, e.g. local_setup1 or make code work without?
    di = DandiDatasetter(
        dandi_client=text_dandiset.client,
        target_path=tmp_path,
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
    assert not (tmp_path / "999999").exists()

    # Since we are using text_dandiset, that immediately creates us a dandiset
    # TODO: may be separate it out, so we could start "clean" and still work ok
    # clean run without dandisets is ok
    # ret = di.update_from_backup()
    # assert ret is None, "nothing is returned ATM, if added -- test should be extended"

    dandiset_id = text_dandiset.dandiset_id
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])

    ds = Dataset(tmp_path / dandiset_id)  # but we should get the super-dataset?
    assert_repo_status(ds.path)  # that all is clean etc
    ok_file_under_git(ds.path, "file.txt")

    (text_dandiset.dspath / "new.txt").write_text("This is a new file.\n")
    log.info("test_1: Updating test dandiset on server")
    text_dandiset.upload()
    assert_repo_status(ds.path)  # no side-effects somehow
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    assert (ds.pathobj / "new.txt").read_text() == "This is a new file.\n"

    repo = GitRepo(ds.path)

    def check_version_tag(v: Version) -> None:
        vid = v.identifier

        # Assert tag has correct timestamp
        assert repo.get_tag_date(vid) == v.created.isoformat(timespec="seconds")

        # Assert tag has correct committer
        assert repo.get_tag_creator(vid) == "DANDI User <info@dandiarchive.org>"

        # Assert tagged commit has correct timestamp
        assert repo.get_commit_date(vid) == v.created.isoformat(timespec="seconds")

        # Assert that tag was merged into default branch
        assert repo.is_ancestor(vid, DEFAULT_BRANCH)

        # Assert tag branches from default branch
        assert repo.parent_is_ancestor(DEFAULT_BRANCH, vid)

        # Assert dandiset.yaml in tagged commit has doi
        metadata = yaml_load(repo.get_blob(vid, dandiset_metadata_file))
        assert metadata.get("doi")

    log.info("test_1: Waiting for Dandiset to become valid")
    text_dandiset.dandiset.wait_until_valid(65)
    log.info("test_1: Publishing Dandiset")
    v1 = text_dandiset.dandiset.publish().version
    version1 = v1.identifier
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    tags = {t["name"]: t["hexsha"] for t in ds.repo.get_tags()}
    assert version1 in tags
    v1_hash = tags[version1]
    check_version_tag(v1)

    (text_dandiset.dspath / "new.txt").write_text(
        "This file's contents were changed.\n"
    )
    log.info("test_1: Updating test dandiset on server")
    text_dandiset.upload()
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    assert (
        ds.pathobj / "new.txt"
    ).read_text() == "This file's contents were changed.\n"

    log.info("test_1: Waiting for Dandiset to become valid")
    text_dandiset.dandiset.wait_until_valid(65)
    log.info("test_1: Publishing Dandiset")
    v2 = text_dandiset.dandiset.publish().version
    version2 = v2.identifier
    log.info("test_1: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    tags = {t["name"]: t["hexsha"] for t in ds.repo.get_tags()}
    assert version1 in tags
    assert tags[version1] == v1_hash
    assert version2 in tags
    check_version_tag(v2)

    commit_authors = repo.readcmd(
        "log", "--no-merges", "--format=%an <%ae>"
    ).splitlines()
    assert commit_authors == ["DANDI User <info@dandiarchive.org>"] * len(
        commit_authors
    )

    for c in repo.get_backup_commits():
        assert repo.get_asset_files(c) == {
            asset["path"] for asset in repo.get_assets_json(c)
        }


def test_2(text_dandiset: SampleDandiset, tmp_path: Path) -> None:
    """
    Test of adding in version-tagging after backups have already been taken.

    This test creates several versions, takes a backup after each one with
    tagging disabled, and then takes another backup with tagging re-enabled.
    """
    di = DandiDatasetter(
        dandi_client=text_dandiset.client,
        target_path=tmp_path,
        config=Config(
            content_url_regex=r".*/blobs/",
            s3bucket="dandi-api-staging-dandisets",
            enable_tags=False,
        ),
    )

    dandiset_id = text_dandiset.dandiset_id
    dspath = text_dandiset.dspath
    dandiset = text_dandiset.dandiset
    log.info("test_2: Creating new backup of Dandiset")
    di.update_from_backup([dandiset_id])
    backupdir = tmp_path / dandiset_id
    repo = GitRepo(backupdir)

    versions = []
    for i in range(1, 4):
        (dspath / "counter.txt").write_text(f"{i}\n")
        for vn in dspath.glob("v*.txt"):
            vn.unlink()
            dandiset.get_asset_by_path(vn.name).delete()
        if i > 1:
            sleep(2)  # Ensure v{i}.txt has a timestamp after the last version
        (dspath / f"v{i}.txt").write_text(f"Version {i}\n")
        # Something goes wrong with the download if v{i} and w{i} have the same
        # content.
        (dspath / f"w{i}.txt").write_text(f"Wersion {i}\n")
        text_dandiset.upload()
        log.info("test_2: Publishing version #%s", i)
        dandiset.wait_until_valid(65)
        v = dandiset.publish().version
        log.info(
            "test_2: Updating backup (release-tagging disabled) for version #%s", i
        )
        di.update_from_backup([dandiset_id])
        assert repo.get_tags() == []
        assert (backupdir / "counter.txt").read_text() == f"{i}\n"
        assert list(backupdir.glob("v*.txt")) == [backupdir / f"v{i}.txt"]
        assert (backupdir / f"v{i}.txt").read_text() == f"Version {i}\n"
        base = repo.get_commitish_hash("HEAD")
        log.info(
            "test_2: Expecting %s tag to be based off commit %s", v.identifier, base
        )
        versions.append((v, base))

    di.config.enable_tags = True
    log.info("test_2: Updating backup, now with release-tagging enabled")
    di.update_from_backup([dandiset_id])

    for i, (v, base) in enumerate(versions, start=1):
        assert (
            repo.get_commit_subject(v.identifier)
            == "[backups2datalad] dandiset.yaml updated"
        )
        assert repo.get_commitish_hash(f"{v.identifier}^") == base
        if i < len(versions):
            assert not repo.is_ancestor(v.identifier, DEFAULT_BRANCH)
        else:
            assert repo.is_ancestor(v.identifier, DEFAULT_BRANCH)

    for c in repo.get_backup_commits():
        assert repo.get_asset_files(c) == {
            asset["path"] for asset in repo.get_assets_json(c)
        }


def test_3(text_dandiset: SampleDandiset, tmp_path: Path) -> None:
    """
    Test of "debouncing" (GH-89, GH-97).

    This test creates several versions and takes a backup afterwards.
    """
    di = DandiDatasetter(
        dandi_client=text_dandiset.client,
        target_path=tmp_path,
        config=Config(
            content_url_regex=r".*/blobs/",
            s3bucket="dandi-api-staging-dandisets",
            enable_tags=True,
        ),
    )
    dandiset_id = text_dandiset.dandiset_id
    dspath = text_dandiset.dspath
    dandiset = text_dandiset.dandiset
    versions = []
    for i in range(1, 4):
        (dspath / "counter.txt").write_text(f"{i}\n")
        for vn in dspath.glob("v*.txt"):
            vn.unlink()
            dandiset.get_asset_by_path(vn.name).delete()
        if i > 1:
            sleep(2)  # Ensure v{i}.txt has a timestamp after the last version
        (dspath / f"v{i}.txt").write_text(f"Version {i}\n")
        # Something goes wrong with the download if v{i} and w{i} have the same
        # content.
        (dspath / f"w{i}.txt").write_text(f"Wersion {i}\n")
        text_dandiset.upload()
        log.info("test_3: Publishing version #%s", i)
        dandiset.wait_until_valid(65)
        if i < 3:
            status = {
                ".dandi/assets.json": "M",
                "counter.txt": "A",
                "dandiset.yaml": "M",
                f"v{i}.txt": "A",
            }
        else:
            status = {"dandiset.yaml": "M"}
        versions.append((dandiset.publish().version, status))
    log.info("test_3: Creating backup of Dandiset")
    di.update_from_backup([dandiset_id])
    repo = GitRepo(tmp_path / dandiset_id)
    for i, (v, status) in enumerate(versions, start=1):
        assert repo.parent_is_ancestor(
            v.identifier, DEFAULT_BRANCH
        ), f"Tag is more than one commit off of {DEFAULT_BRANCH} branch"
        if i < len(versions):
            assert (
                repo.get_commit_subject(v.identifier)
                == "[backups2datalad] 2 files added"
            )
            assert not repo.is_ancestor(v.identifier, DEFAULT_BRANCH)
        else:
            assert (
                repo.get_commit_subject(v.identifier)
                == "[backups2datalad] dandiset.yaml updated"
            )
            assert repo.is_ancestor(v.identifier, DEFAULT_BRANCH)
        assert repo.get_diff_tree(v.identifier) == status

    # Assert each (non-merge) backup commit on the default branch is a parent
    # of a tag (except the last one, which *is* a tag).
    our_commits = repo.readcmd(
        "rev-list", r"--grep=\[backups2datalad\]", "HEAD"
    ).splitlines()
    assert len(our_commits) == len(versions) + 1
    for c, (v, _) in zip(reversed(our_commits), versions):
        assert repo.get_commitish_hash(f"{v.identifier}^") == c
    assert our_commits[0] == repo.get_commitish_hash(versions[-1][0].identifier)

    for c in repo.get_backup_commits():
        assert repo.get_asset_files(c) == {
            asset["path"] for asset in repo.get_assets_json(c)
        }


def test_4(text_dandiset: SampleDandiset, tmp_path: Path) -> None:
    """
    This test creates several versions and takes a backup after each one.
    """
    di = DandiDatasetter(
        dandi_client=text_dandiset.client,
        target_path=tmp_path,
        config=Config(
            content_url_regex=r".*/blobs/",
            s3bucket="dandi-api-staging-dandisets",
            enable_tags=True,
        ),
    )
    dandiset_id = text_dandiset.dandiset_id
    dspath = text_dandiset.dspath
    dandiset = text_dandiset.dandiset
    repo = GitRepo(tmp_path / dandiset_id)
    for i in range(1, 4):
        (dspath / "counter.txt").write_text(f"{i}\n")
        for vn in dspath.glob("v*.txt"):
            vn.unlink()
            dandiset.get_asset_by_path(vn.name).delete()
        if i > 1:
            sleep(2)  # Ensure v{i}.txt has a timestamp after the last version
        (dspath / f"v{i}.txt").write_text(f"Version {i}\n")
        # Something goes wrong with the download if v{i} and w{i} have the same
        # content.
        (dspath / f"w{i}.txt").write_text(f"Wersion {i}\n")
        text_dandiset.upload()
        log.info("test_4: Publishing version #%s", i)
        dandiset.wait_until_valid(65)
        v = dandiset.publish().version
        log.info("test_4: Creating backup of Dandiset")
        di.update_from_backup([dandiset_id])
        assert (
            repo.get_commit_subject(v.identifier)
            == "[backups2datalad] dandiset.yaml updated"
        )
        assert repo.parent_is_ancestor(
            v.identifier, DEFAULT_BRANCH
        ), f"Tag is more than one commit off of {DEFAULT_BRANCH} branch"
        assert repo.is_ancestor(v.identifier, DEFAULT_BRANCH)
    for c in repo.get_backup_commits():
        assert repo.get_asset_files(c) == {
            asset["path"] for asset in repo.get_assets_json(c)
        }


def test_binary(text_dandiset: SampleDandiset, tmp_path: Path) -> None:
    di = DandiDatasetter(
        dandi_client=text_dandiset.client,
        target_path=tmp_path,
        config=Config(
            content_url_regex=r".*/blobs/",
            s3bucket="dandi-api-staging-dandisets",
            enable_tags=True,
        ),
    )
    dandiset_id = text_dandiset.dandiset_id
    dspath = text_dandiset.dspath
    (dspath / "data.dat").write_bytes(b"\0\1\2\3\4\5")
    text_dandiset.upload()
    log.info("test_binary: Syncing test dandiset")
    di.update_from_backup([dandiset_id])
    backup = tmp_path / dandiset_id
    annex = AnnexRepo(backup)
    data_backup = backup / "data.dat"
    assert data_backup.is_symlink() and not data_backup.is_file()
    assert annex.is_under_annex([data_backup]) == [True]


def test_backup_command(text_dandiset: SampleDandiset, tmp_path: Path) -> None:
    r = CliRunner().invoke(
        main,
        [
            "--dandi-instance",
            "dandi-staging",
            "--target",
            str(tmp_path / "ds"),
            "--s3bucket",
            "dandi-api-staging-dandisets",
            "update-from-backup",
            "--zarr-target",
            str(tmp_path / "zarr"),
            text_dandiset.dandiset_id,
        ],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    text_files = {
        Path(f).relative_to(text_dandiset.dspath).as_posix()
        for f in find_files(
            r".*",
            [text_dandiset.dspath],
            exclude_dotfiles=False,
            exclude_dotdirs=False,
            exclude_vcs=False,
        )
    }
    ds = Dataset(tmp_path / "ds" / text_dandiset.dandiset_id)
    assert ds.is_installed()
    assert_repo_status(ds.path)
    sync_files = {f for f in ds.repo.get_files() if not is_meta_file(f)}
    assert sync_files == text_files
    for e in sync_files:
        p = ds.pathobj / e
        assert p.is_file()
        if e != dandiset_metadata_file:
            assert p.read_text() == (text_dandiset.dspath / e).read_text()
    assert not any(ds.repo.is_under_annex(list(sync_files)))


def test_custom_commit_date(tmp_path: Path) -> None:
    ds = Dataset(tmp_path)
    ds.create(cfg_proc="text2git")
    (tmp_path / "file.txt").write_text("This is test text.\n")
    with custom_commit_date(datetime(2021, 6, 1, 12, 34, 56, tzinfo=timezone.utc)):
        ds.save(message="Add a file")
    repo = GitRepo(tmp_path)
    assert repo.get_commit_date("HEAD") == "2021-06-01T12:34:56+00:00"
    assert repo.get_commit_author("HEAD") == "DANDI User <info@dandiarchive.org>"
