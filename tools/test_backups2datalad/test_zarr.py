from __future__ import annotations

import logging
from operator import itemgetter
from pathlib import Path
from shutil import rmtree
from typing import Optional

from conftest import SampleDandiset
from dandi.utils import find_files
from datalad.api import Dataset
from datalad.tests.utils import assert_repo_status
import numpy as np
import pytest
from test_util import GitRepo
import zarr

from backups2datalad.adataset import AsyncDataset
from backups2datalad.config import BackupConfig, ResourceConfig
from backups2datalad.datasetter import DandiDatasetter, DandisetStats
from backups2datalad.logging import log as plog
from backups2datalad.util import is_meta_file
from backups2datalad.zarr import CHECKSUM_FILE, sync_zarr

log = logging.getLogger("test_backups2datalad.test_zarr")

pytestmark = pytest.mark.anyio


def check_zarr(
    source_path: Path, zarrds: Dataset, checksum: Optional[str] = None
) -> None:
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
    sync_entries = {f for f in zarrds.repo.get_files() if not is_meta_file(f)}
    assert sync_entries == zarr_entries
    for e in sync_entries:
        p = zarrds.pathobj / e
        assert p.is_symlink() and not p.is_file()
    assert all(zarrds.repo.is_under_annex(list(sync_entries)))
    assert (zarrds.pathobj / CHECKSUM_FILE).is_file()
    if checksum is not None:
        assert (zarrds.pathobj / CHECKSUM_FILE).read_text().strip() == checksum
    else:
        assert (zarrds.pathobj / CHECKSUM_FILE).exists()
    assert zarrds.repo.is_under_annex([str(CHECKSUM_FILE)]) == [False]


async def test_sync_zarr(new_dandiset: SampleDandiset, tmp_path: Path) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr.save(zarr_path, np.arange(1000), np.arange(1000, 0, -1))
    await new_dandiset.upload()
    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    checksum = asset.get_digest().value
    config = BackupConfig(
        s3bucket="dandi-api-staging-dandisets", zarrs=ResourceConfig(path="zarrs")
    )
    await sync_zarr(asset, checksum, tmp_path, config, plog)
    check_zarr(zarr_path, Dataset(tmp_path), checksum)


async def test_backup_zarr(new_dandiset: SampleDandiset, tmp_path: Path) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr.save(zarr_path, np.arange(1000), np.arange(1000, 0, -1))
    (new_dandiset.dspath / "file.txt").write_text("This is test text.\n")
    await new_dandiset.upload()
    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")

    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        config=BackupConfig(
            backup_root=tmp_path,
            dandi_instance="dandi-staging",
            s3bucket="dandi-api-staging-dandisets",
            dandisets=ResourceConfig(path="ds"),
            zarrs=ResourceConfig(path="zarrs"),
        ),
    )
    dandiset_id = new_dandiset.dandiset_id
    log.info("test_backup_zarr: Syncing Zarr dandiset")
    await di.update_from_backup([dandiset_id])

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
    assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 2 files added"
    assert {asset["path"] for asset in gitrepo.get_assets_json("HEAD")} == {
        "file.txt",
        "sample.zarr",
    }

    assert await di.get_dandiset_stats(AsyncDataset(ds.pathobj)) == (
        DandisetStats(files=6, size=1535),
        {asset.zarr: DandisetStats(files=5, size=1516)},
    )

    log.info("test_backup_zarr: Syncing unmodified Zarr dandiset")
    await di.update_from_backup([dandiset_id])

    check_zarr(zarr_path, zarrds, checksum=asset.get_digest().value)
    (submod,) = ds.repo.get_submodules_()
    assert submod["path"] == ds.pathobj / "sample.zarr"
    assert submod["gitmodule_url"] == str(zarrds.pathobj)
    assert submod["type"] == "dataset"
    assert submod["gitshasum"] == zarrds.repo.format_commit("%H")
    c = gitrepo.get_commit_count()
    if c == 4:
        # dandiset.yaml was updated again during the second backup because the
        # server took a while to incorporate the Zarr size data
        bump = 1
        assert (
            gitrepo.get_commit_subject("HEAD")
            == "[backups2datalad] Only some metadata updates"
        )
    else:
        bump = 0
        assert c == 3
        assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 2 files added"
    assert {asset["path"] for asset in gitrepo.get_assets_json("HEAD")} == {
        "file.txt",
        "sample.zarr",
    }
    assert zarrgit.get_commit_count() == 3

    rmtree(zarr_path)
    zarr.save(zarr_path, np.eye(5))
    await new_dandiset.upload()
    log.info("test_backup_zarr: Syncing modified Zarr dandiset")
    await di.update_from_backup([dandiset_id])

    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    check_zarr(zarr_path, zarrds, checksum=asset.get_digest().value)
    (submod,) = ds.repo.get_submodules_()
    assert submod["path"] == ds.pathobj / "sample.zarr"
    assert submod["gitmodule_url"] == str(zarrds.pathobj)
    assert submod["type"] == "dataset"
    assert submod["gitshasum"] == zarrds.repo.format_commit("%H")
    assert gitrepo.get_commit_count() == 4 + bump
    assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 1 file updated"
    assert {asset["path"] for asset in gitrepo.get_assets_json("HEAD")} == {
        "file.txt",
        "sample.zarr",
    }
    assert zarrgit.get_commit_count() == 4


async def test_backup_zarr_entry_conflicts(
    new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr_path.mkdir()
    (zarr_path / "changed01").mkdir()
    (zarr_path / "changed01" / "file.txt").write_text("This is test text.\n")
    (zarr_path / "changed02").write_text("This is also test text.\n")
    await new_dandiset.upload()

    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        config=BackupConfig(
            backup_root=tmp_path,
            dandi_instance="dandi-staging",
            s3bucket="dandi-api-staging-dandisets",
            dandisets=ResourceConfig(path="ds"),
            zarrs=ResourceConfig(path="zarrs"),
        ),
    )
    dandiset_id = new_dandiset.dandiset_id
    log.info("test_backup_zarr_entry_conflicts: Syncing Zarr dandiset")
    await di.update_from_backup([dandiset_id])

    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    zarrds = Dataset(tmp_path / "zarrs" / asset.zarr)
    check_zarr(zarr_path, zarrds, checksum=asset.get_digest().value)

    rmtree(zarr_path)
    zarr_path.mkdir()
    (zarr_path / "changed01").write_text("This is now a file.\n")
    (zarr_path / "changed02").mkdir()
    (zarr_path / "changed02" / "file.txt").write_text(
        "The parent is now a directory.\n"
    )
    await new_dandiset.upload()

    log.info("test_backup_zarr_entry_conflicts: Syncing modified Zarr dandiset")
    await di.update_from_backup([dandiset_id])

    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    check_zarr(zarr_path, zarrds, checksum=asset.get_digest().value)


async def test_backup_zarr_delete_zarr(
    new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr.save(zarr_path, np.arange(1000), np.arange(1000, 0, -1))
    await new_dandiset.upload()

    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        config=BackupConfig(
            backup_root=tmp_path,
            dandi_instance="dandi-staging",
            s3bucket="dandi-api-staging-dandisets",
            dandisets=ResourceConfig(path="ds"),
            zarrs=ResourceConfig(path="zarrs"),
        ),
    )

    dandiset_id = new_dandiset.dandiset_id

    log.info("test_backup_zarr_delete_zarr: Syncing Zarr dandiset")
    await di.update_from_backup([dandiset_id])

    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    await new_dandiset.client.delete(asset.api_path)

    log.info("test_backup_zarr_delete_zarr: Syncing Zarr dandiset after deleting Zarr")
    await di.update_from_backup([dandiset_id])
    assert not (tmp_path / "ds" / dandiset_id / "sample.zarr").exists()
    gitrepo = GitRepo(tmp_path / "ds" / dandiset_id)
    assert gitrepo.get_commit_subject("HEAD") == "[backups2datalad] 1 file deleted"


async def test_backup_zarr_pathological(
    new_dandiset: SampleDandiset, tmp_path: Path
) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr.save(zarr_path, np.arange(1000), np.arange(1000, 0, -1))
    await new_dandiset.upload()

    client = new_dandiset.client
    dandiset_id = new_dandiset.dandiset_id
    asset = await new_dandiset.dandiset.aget_asset_by_path("sample.zarr")
    sample_zarr_id = asset.zarr

    await client.post(
        f"{new_dandiset.dandiset.version_api_path}assets/",
        json={"metadata": {"path": "link.zarr"}, "zarr_id": sample_zarr_id},
    )

    r = await client.post(
        "/zarr/", json={"name": "empty.zarr", "dandiset": dandiset_id}
    )
    empty_zarr_id = r["zarr_id"]
    await client.post(
        f"{new_dandiset.dandiset.version_api_path}assets/",
        json={"metadata": {"path": "empty.zarr"}, "zarr_id": empty_zarr_id},
    )

    di = DandiDatasetter(
        dandi_client=new_dandiset.client,
        config=BackupConfig(
            backup_root=tmp_path,
            dandi_instance="dandi-staging",
            s3bucket="dandi-api-staging-dandisets",
            dandisets=ResourceConfig(path="ds"),
            zarrs=ResourceConfig(path="zarrs"),
        ),
    )

    log.info("test_backup_zarr_pathological: Syncing Zarr dandiset")
    await di.update_from_backup([dandiset_id])

    sample_zarrds = Dataset(tmp_path / "zarrs" / sample_zarr_id)
    check_zarr(zarr_path, sample_zarrds, checksum=asset.get_digest().value)

    (tmp_path / "empty").mkdir()
    empty_zarrds = Dataset(tmp_path / "zarrs" / empty_zarr_id)
    check_zarr(tmp_path / "empty", empty_zarrds)

    ds = Dataset(tmp_path / "ds" / dandiset_id)
    assert_repo_status(ds.path)

    emptymod, linkmod, samplemod = sorted(
        ds.repo.get_submodules_(), key=itemgetter("path")
    )

    assert emptymod["path"] == ds.pathobj / "empty.zarr"
    assert emptymod["gitmodule_url"] == str(empty_zarrds.pathobj)
    assert emptymod["type"] == "dataset"
    assert emptymod["gitshasum"] == empty_zarrds.repo.format_commit("%H")

    assert linkmod["path"] == ds.pathobj / "link.zarr"
    assert linkmod["gitmodule_url"] == str(sample_zarrds.pathobj)
    assert linkmod["type"] == "dataset"
    assert linkmod["gitshasum"] == sample_zarrds.repo.format_commit("%H")

    assert samplemod["path"] == ds.pathobj / "sample.zarr"
    assert samplemod["gitmodule_url"] == str(sample_zarrds.pathobj)
    assert samplemod["type"] == "dataset"
    assert samplemod["gitshasum"] == sample_zarrds.repo.format_commit("%H")
