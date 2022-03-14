import logging
from pathlib import Path
from shutil import rmtree

from conftest import SampleDandiset
from dandi.utils import find_files
from datalad.api import Dataset
from datalad.tests.utils import assert_repo_status
import numpy as np
from test_util import GitRepo
import trio
import zarr

from backups2datalad.datasetter import DandiDatasetter
from backups2datalad.util import Config
from backups2datalad.zarr import CHECKSUM_FILE, sync_zarr

log = logging.getLogger("test_backups2datalad.test_zarr")


def check_zarr(source_path: Path, zarrds: Dataset, checksum: str) -> None:
    zarr_entries = {
        Path(f).relative_to(source_path).as_posix()
        for f in find_files(
            r".*",
            [source_path],
            exclude_dotfiles=False,
            exclude_dotdirs=False,
            exclude_vcs=False,
        )
    }
    assert zarrds.is_installed()
    assert_repo_status(zarrds.path)
    sync_entries = {
        f
        for f in zarrds.repo.get_files()
        if f != ".gitattributes"
        and f != CHECKSUM_FILE
        and not f.startswith(".datalad/")
    }
    assert sync_entries == zarr_entries
    for e in sync_entries:
        p = zarrds.pathobj / e
        assert p.is_symlink() and not p.is_file()
    assert all(zarrds.repo.is_under_annex(list(sync_entries)))
    assert (zarrds.pathobj / CHECKSUM_FILE).is_file()
    assert (zarrds.pathobj / CHECKSUM_FILE).read_text().strip() == checksum
    assert zarrds.repo.is_under_annex([CHECKSUM_FILE]) == [False]


def test_sync_zarr(new_dandiset: SampleDandiset, tmp_path: Path) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr.save(zarr_path, np.arange(1000), np.arange(1000, 0, -1))
    new_dandiset.upload()
    asset = new_dandiset.dandiset.get_asset_by_path("sample.zarr")
    checksum = asset.get_digest().value
    config = Config(s3bucket="dandi-api-staging-dandisets")
    trio.run(sync_zarr, asset, checksum, tmp_path, config)
    check_zarr(zarr_path, Dataset(tmp_path), checksum)


def test_backup_zarr(new_dandiset: SampleDandiset, tmp_path: Path) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr.save(zarr_path, np.arange(1000), np.arange(1000, 0, -1))
    new_dandiset.upload()
    asset = new_dandiset.dandiset.get_asset_by_path("sample.zarr")
    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        target_path=tmp_path / "ds",
        config=Config(
            content_url_regex=r".*/blobs/",
            s3bucket="dandi-api-staging-dandisets",
            zarr_target=tmp_path / "zarrs",
        ),
    )
    dandiset_id = new_dandiset.dandiset_id
    log.info("test_backup_zarr: Syncing Zarr dandiset")
    di.update_from_backup([dandiset_id])

    zarrds = Dataset(tmp_path / "zarrs" / asset.zarr)
    check_zarr(zarr_path, zarrds, checksum=asset.get_digest().value)
    zarrgit = GitRepo(zarrds.pathobj)
    assert zarrgit.get_commit_count() == 3

    ds = Dataset(tmp_path / "ds" / dandiset_id)
    assert_repo_status(ds.path)

    (submod,) = ds.repo.get_submodules_()
    assert submod["path"] == ds.pathobj / "sample.zarr"
    assert submod["gitmodule_url"] == str(zarrds.pathobj)
    assert submod["type"] == "dataset"
    assert submod["gitshasum"] == zarrds.repo.format_commit("%H")

    gitrepo = GitRepo(ds.pathobj)
    assert gitrepo.get_commit_count() == 3
    assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 1 file added"
    assert {asset["path"] for asset in gitrepo.get_assets_json("HEAD")} == {
        "sample.zarr"
    }

    rmtree(zarr_path)
    zarr.save(zarr_path, np.eye(5))
    new_dandiset.upload()
    log.info("test_backup_zarr: Syncing modified Zarr dandiset")
    di.update_from_backup([dandiset_id])

    asset = new_dandiset.dandiset.get_asset_by_path("sample.zarr")
    check_zarr(zarr_path, zarrds, checksum=asset.get_digest().value)
    (submod,) = ds.repo.get_submodules_()
    assert submod["path"] == ds.pathobj / "sample.zarr"
    assert submod["gitmodule_url"] == str(zarrds.pathobj)
    assert submod["type"] == "dataset"
    assert submod["gitshasum"] == zarrds.repo.format_commit("%H")
    assert gitrepo.get_commit_count() == 4
    assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 1 file updated"
    assert {asset["path"] for asset in gitrepo.get_assets_json("HEAD")} == {
        "sample.zarr"
    }
    assert zarrgit.get_commit_count() == 4
