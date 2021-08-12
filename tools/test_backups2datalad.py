import logging
import os

import pytest

from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import DandiAPIClient
from dandi.upload import upload

# needs nose
from datalad.api import Dataset
from datalad.tests.utils import assert_repo_status, ok_file_under_git

from backups2datalad import DandiDatasetter

@pytest.fixture(autouse=True)
def capture_all_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="backups2datalad")

@pytest.fixture(scope="session")
def dandi_client():
    api_token = os.environ["DANDI_API_KEY"]
    with DandiAPIClient.for_dandi_instance("dandi-staging", token=api_token) as client:
        yield client


@pytest.fixture()
def text_dandiset(dandi_client, tmp_path_factory):
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

    def upload_dandiset(paths=None, **kwargs):
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
    return {
        "client": dandi_client,
        "dspath": dspath,
        "dandiset": d,
        "dandiset_id": dandiset_id,
        "reupload": upload_dandiset,
    }


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
        content_url_regex=r".*/blobs/",
        s3bucket="dandi-api-staging-dandisets",
    )

    with pytest.raises(Exception):
        di.update_from_backup(["999999"])
    assert not (target_path / "999999").exists()

    # Since we are using text_dandiset, that immediately creates us a dandiset
    # TODO: may be separate it out, so we could start "clean" and still work ok
    # clean run without dandisets is ok
    # ret = di.update_from_backup()
    # assert ret is None, "nothing is returned ATM, if added -- test should be extended"

    dandiset_id = text_dandiset["dandiset_id"]
    di.update_from_backup([dandiset_id])

    ds = Dataset(
        target_path / text_dandiset["dandiset_id"]
    )  # but we should get the super-dataset?
    assert_repo_status(ds.path)  # that all is clean etc
    ok_file_under_git(ds.path, "file.txt")

    (text_dandiset["dspath"] / "new.txt").write_text("This is a new file.\n")
    text_dandiset["reupload"]()
    assert_repo_status(ds.path)  # no side-effects somehow
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    assert (ds.pathobj / "new.txt").read_text() == "This is a new file.\n"

    version1 = text_dandiset["dandiset"].publish().version.identifier
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    assert version1 in [t["name"] for t in ds.repo.get_tags()]

    (text_dandiset["dspath"] / "new.txt").write_text(
        "This file's contents were changed.\n"
    )
    text_dandiset["reupload"]()
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    assert (ds.pathobj / "new.txt").read_text() == "This file's contents were changed.\n"

    version2 = text_dandiset["dandiset"].publish().version.identifier
    di.update_from_backup([dandiset_id])
    assert_repo_status(ds.path)  # that all is clean etc
    tagnames = [t["name"] for t in ds.repo.get_tags()]
    assert version1 in tagnames
    assert version2 in tagnames
