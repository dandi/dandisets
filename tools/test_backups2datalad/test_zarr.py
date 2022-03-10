from pathlib import Path

from conftest import SampleDandiset
from dandi.utils import find_files
from datalad.api import Dataset
import numpy as np
import trio
import zarr

from backups2datalad.util import Config
from backups2datalad.zarr import CHECKSUM_FILE, sync_zarr


def test_sync_zarr(new_dandiset: SampleDandiset, tmp_path: Path) -> None:
    zarr_path = new_dandiset.dspath / "sample.zarr"
    zarr.save(zarr_path, np.arange(1000), np.arange(1000, 0, -1))
    new_dandiset.upload()

    zarr_entries = {
        Path(f).relative_to(zarr_path).as_posix()
        for f in find_files(
            r".*",
            [zarr_path],
            exclude_dotfiles=False,
            exclude_dotdirs=False,
            exclude_vcs=False,
        )
    }

    asset = new_dandiset.dandiset.get_asset_by_path("sample.zarr")
    checksum = asset.get_digest().value
    config = Config(s3bucket="dandi-api-staging-dandisets")
    trio.run(sync_zarr, asset, checksum, tmp_path, config)

    ds = Dataset(tmp_path)
    assert ds.is_installed()
    sync_entries = {
        f
        for f in ds.repo.get_files()
        if f != ".gitattributes"
        and f != CHECKSUM_FILE
        and not f.startswith(".datalad/")
    }

    assert sync_entries == zarr_entries
    for e in sync_entries:
        p = ds.pathobj / e
        assert p.is_symlink() and not p.is_file()
    assert all(ds.repo.is_under_annex(list(sync_entries)))

    assert (ds.pathobj / CHECKSUM_FILE).is_file()
    assert (ds.pathobj / CHECKSUM_FILE).read_text().strip() == checksum
    assert ds.repo.is_under_annex([CHECKSUM_FILE]) == [False]
