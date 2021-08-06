from backups2datalad import DandiDatasetter
from datalad.api import Dataset
from pathlib import Path


import pytest

# all imports seems needed for pytest to discover those fixtures
from dandi.tests.fixtures import (
    docker_compose_setup,
    local_dandi_api,
    text_dandiset,
)

# needs  nose
from datalad.tests.utils import (
    assert_repo_status,
    ok_file_under_git
)


def test_1(text_dandiset, tmpdir):
    # TODO: move pre-setup into a fixture, e.g. local_setup1 or make code work without?
    assetstore_path = tmpdir / "assetstore"
    assetstore_path.mkdir().mkdir("girder-assetstore")
    target_path = Path(tmpdir / "target")
    di = DandiDatasetter(
        dandi_client=text_dandiset["client"],
        assetstore_path=Path(assetstore_path),
        target_path=target_path,
        ignore_errors=False,
        # gh_org=None,
        # re_filter=None,
        # backup_remote=None,
        # jobs=jobs,
        # force=force,
        # update_asset_meta_version=update_asset_meta_version,
        content_url_regex=r".*/blobs/"
    )
    # some exception is raised whenever we try to update non-existing dandiset
    with pytest.raises(Exception):
        di.update_from_backup(['000999'])

    # TODO: fix -- should not initiate dataset unless "valid"
    # assert not (target_path / "000999").exists()

    # Since we are using text_dandiset, that immediately creates us a dandiset
    # TODO: may be separate it out, so we could start "clean" and still work ok
    # clean run without dandisets is ok
    # ret = di.update_from_backup()
    # assert ret is None,  "nothing is returned ATM, if added -- test should be extended"

    di.update_from_backup()

    ds = Dataset(target_path / text_dandiset["dandiset_id"])  # but we should get the super-dataset?
    assert_repo_status(ds.path)  # that all is clean etc

    # TODO: check that files were committed, ok_file_under_git could be of help

    # TODO: add changes to the dandiset

    # reupload dandiset
    text_dandiset["reupload"]()

    assert_repo_status(ds.path)  # no side-effects somehow

    di.update_from_backup()
    assert_repo_status(ds.path)  # that all is clean etc

    # TODO: check that added file was uploaded/fetched

    # TODO: publish the dandiset
    version1 = '???'

    di.update_from_backup()
    assert_repo_status(ds.path)  # that all is clean etc
    assert version1 in ds.repo.get_tags()

    # TODO: more modifications to draft

    di.update_from_backup()
    assert_repo_status(ds.path)  # that all is clean etc
    # TODO: check modifications being in place

    # TODO: another publish and another check for now both tags being present