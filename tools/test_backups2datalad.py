from pathlib import Path

import pytest

# all imports seems needed for pytest to discover those fixtures
from dandi.tests.fixtures import (
    docker_compose_setup,
    local_dandi_api,
    text_dandiset,
)

# needs  nose
from datalad.api import Dataset
from datalad.tests.utils import assert_repo_status, ok_file_under_git

from backups2datalad import DandiDatasetter


def test_1(text_dandiset, tmp_path):
    # TODO: move pre-setup into a fixture, e.g. local_setup1 or make code work without?
    assetstore_path = tmp_path / "assetstore"
    assetstore_path.mkdir()
    (assetstore_path / "girder-assetstore").mkdir()
    target_path = tmp_path / "target"
    di = DandiDatasetter(
        dandi_client=text_dandiset["client"],
        assetstore_path=assetstore_path,
        target_path=target_path,
        ignore_errors=False,
        # gh_org=None,
        # re_filter=None,
        # backup_remote=None,
        # jobs=jobs,
        # force=force,
        # update_asset_meta_version=update_asset_meta_version,
        content_url_regex=r".*/blobs/",
    )

    with pytest.raises(Exception):
        di.update_from_backup(["000999"])
    assert not (target_path / "000999").exists()

    # Since we are using text_dandiset, that immediately creates us a dandiset
    # TODO: may be separate it out, so we could start "clean" and still work ok
    # clean run without dandisets is ok
    # ret = di.update_from_backup()
    # assert ret is None, "nothing is returned ATM, if added -- test should be extended"

    di.update_from_backup()

    ds = Dataset(
        target_path / text_dandiset["dandiset_id"]
    )  # but we should get the super-dataset?
    assert_repo_status(ds.path)  # that all is clean etc
    ok_file_under_git(ds.path, "file.txt")

    (text_dandiset["dspath"] / "new.txt").write_text("This is a new file.\n")
    text_dandiset["reupload"]()
    assert_repo_status(ds.path)  # no side-effects somehow
    di.update_from_backup()
    assert_repo_status(ds.path)  # that all is clean etc
    assert (ds.path / "new.txt").read_text() == "This is a new file.\n"

    version1 = text_dandiset["dandiset"].publish().version.identifier
    di.update_from_backup()
    assert_repo_status(ds.path)  # that all is clean etc
    assert version1 in ds.repo.get_tags()

    (text_dandiset["dspath"] / "new.txt").write_text(
        "This file's contents were changed.\n"
    )
    text_dandiset["reupload"]()
    di.update_from_backup()
    assert_repo_status(ds.path)  # that all is clean etc
    assert (ds.path / "new.txt").read_text() == "This file's contents were changed.\n"

    version2 = text_dandiset["dandiset"].publish().version.identifier
    di.update_from_backup()
    assert_repo_status(ds.path)  # that all is clean etc
    assert version1 in ds.repo.get_tags()
    assert version2 in ds.repo.get_tags()
