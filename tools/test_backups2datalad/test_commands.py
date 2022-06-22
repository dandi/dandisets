from __future__ import annotations

import os
from pathlib import Path
from traceback import format_exception

from asyncclick.testing import CliRunner, Result
from conftest import SampleDandiset
from dandi.consts import dandiset_metadata_file
from datalad.api import Dataset
from datalad.tests.utils import assert_repo_status
import numpy as np
import pytest
from test_util import find_filepaths
import zarr

from backups2datalad.__main__ import main
from backups2datalad.config import BackupConfig, Remote, ResourceConfig
from backups2datalad.util import is_meta_file

pytestmark = pytest.mark.anyio


def show_result(r: Result) -> str:
    if r.exception is not None:
        assert isinstance(r.exc_info, tuple)
        return "".join(format_exception(*r.exc_info))
    else:
        return r.output


async def test_backup_command(text_dandiset: SampleDandiset, tmp_path: Path) -> None:
    cfgfile = tmp_path / "config.yaml"
    cfgfile.write_text(
        "dandi_instance: dandi-staging\n"
        "s3bucket: dandi-api-staging-dandisets\n"
        "dandisets:\n"
        "  path: ds\n"
    )
    r = await CliRunner().invoke(
        main,
        [
            "--backup-root",
            str(tmp_path),
            "-c",
            str(cfgfile),
            "update-from-backup",
            text_dandiset.dandiset_id,
        ],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)
    text_files = {
        f.relative_to(text_dandiset.dspath).as_posix()
        for f in find_filepaths(text_dandiset.dspath)
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


async def test_populate(new_dandiset: SampleDandiset, tmp_path: Path) -> None:
    TEXT_FILES = {
        "file.txt": "This is test text.\n",
        "fruit/apple.txt": "Apple\n",
        "fruit/banana.txt": "Banana\n",
    }
    BINARY_FILES = {
        "nulls.dat": b"\0\0\0\0\0",
        "hi.txt.gz": bytes.fromhex(
            "1f 8b 08 08 0b c1 a0 62 00 03 68 69 2e 74 78 74"
            "00 f3 c8 e4 02 00 9a 3c 22 d5 03 00 00 00"
        ),
        "img/png/pixel.png": bytes.fromhex(
            "89 50 4e 47 0d 0a 1a 0a 00 00 00 0d 49 48 44 52"
            "00 00 00 01 00 00 00 01 01 00 00 00 00 37 6e f9"
            "24 00 00 00 0a 49 44 41 54 08 99 63 68 00 00 00"
            "82 00 81 cb 13 b2 61 00 00 00 00 49 45 4e 44 ae"
            "42 60 82"
        ),
    }
    for fname, txt in TEXT_FILES.items():
        p = new_dandiset.dspath / fname
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(txt)
    for fname, blob in BINARY_FILES.items():
        p = new_dandiset.dspath / fname
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(blob)
    sample_zarr_path = new_dandiset.dspath / "z" / "sample.zarr"
    zarr.save(sample_zarr_path, np.arange(1000), np.arange(1000, 0, -1))
    SAMPLE_ZARR_ENTRIES = {
        f.relative_to(sample_zarr_path).as_posix(): f.read_bytes()
        for f in find_filepaths(sample_zarr_path)
    }
    eye_zarr_path = new_dandiset.dspath / "z" / "eye.zarr"
    zarr.save(eye_zarr_path, np.eye(5))
    EYE_ZARR_ENTRIES = {
        f.relative_to(eye_zarr_path).as_posix(): f.read_bytes()
        for f in find_filepaths(eye_zarr_path)
    }
    await new_dandiset.upload()

    backup_root = tmp_path / "backup"
    remote_root = tmp_path / "remote"
    (remote_root / "ds").mkdir(parents=True, exist_ok=True)
    (remote_root / "zarr").mkdir(parents=True, exist_ok=True)

    cfgfile = tmp_path / "config.yaml"
    cfg = BackupConfig(
        dandi_instance="dandi-staging",
        s3bucket="dandi-api-staging-dandisets",
        backup_root=backup_root,
        dandisets=ResourceConfig(
            path="ds",
            remote=Remote(
                name="remote-dandisets",
                type="directory",
                options={
                    "directory": str(remote_root / "ds"),
                    "encryption": "none",
                },
            ),
        ),
        zarrs=ResourceConfig(
            path="zarr",
            remote=Remote(
                name="remote-zarrs",
                type="directory",
                options={
                    "directory": str(remote_root / "zarr"),
                    "encryption": "none",
                },
            ),
        ),
    )
    cfg.dump_yaml(cfgfile)

    r = await CliRunner().invoke(
        main,
        ["-c", str(cfgfile), "update-from-backup", new_dandiset.dandiset_id],
        standalone_mode=False,
    )
    assert r.exit_code == 0, show_result(r)

    ds = Dataset(backup_root / "ds" / new_dandiset.dandiset_id)
    assert ds.is_installed()
    assert_repo_status(ds.path)
    sync_files = {f for f in ds.repo.get_files() if not is_meta_file(f)}
    assert sync_files == (
        TEXT_FILES.keys()
        | BINARY_FILES.keys()
        | {dandiset_metadata_file, "z/sample.zarr", "z/eye.zarr"}
    )

    assert not any(ds.repo.is_under_annex(list(TEXT_FILES)))
    for fname, txt in TEXT_FILES.items():
        p = ds.pathobj / fname
        assert p.is_file()
        assert p.read_text() == txt

    assert all(ds.repo.is_under_annex(list(BINARY_FILES)))
    keys2blobs: dict[str, bytes] = {}
    for fname, blob in BINARY_FILES.items():
        p = ds.pathobj / fname
        assert p.is_symlink() and not p.exists()
        keys2blobs[Path(os.readlink(p)).name] = blob

    sample_zarr_asset = await new_dandiset.dandiset.aget_asset_by_path("z/sample.zarr")
    eye_zarr_asset = await new_dandiset.dandiset.aget_asset_by_path("z/eye.zarr")
    zarr_keys2blobs: dict[str, bytes] = {}
    for zarr_id, entries in [
        (sample_zarr_asset.zarr, SAMPLE_ZARR_ENTRIES),
        (eye_zarr_asset.zarr, EYE_ZARR_ENTRIES),
    ]:
        zarr_ds = Dataset(backup_root / "zarr" / zarr_id)
        assert zarr_ds.is_installed()
        assert_repo_status(zarr_ds.path)
        zarr_files = {f for f in zarr_ds.repo.get_files() if not is_meta_file(f)}
        assert zarr_files == entries.keys()
        assert all(zarr_ds.repo.is_under_annex(list(zarr_files)))
        for fname, blob in entries.items():
            p = zarr_ds.pathobj / fname
            assert p.is_symlink() and not p.exists()
            zarr_keys2blobs[Path(os.readlink(p)).name] = blob

    r = await CliRunner().invoke(
        main, ["-c", str(cfgfile), "populate"], standalone_mode=False
    )
    assert r.exit_code == 0, show_result(r)
    remote_files = {f.name: f for f in find_filepaths(remote_root / "ds")}
    assert remote_files.keys() == keys2blobs.keys()
    for key, blob in keys2blobs.items():
        assert remote_files[key].read_bytes() == blob

    r = await CliRunner().invoke(
        main, ["-c", str(cfgfile), "populate-zarrs"], standalone_mode=False
    )
    assert r.exit_code == 0, show_result(r)
    zarr_remote_files = {f.name: f for f in find_filepaths(remote_root / "zarr")}
    assert zarr_remote_files.keys() == zarr_keys2blobs.keys()
    for key, blob in zarr_keys2blobs.items():
        assert zarr_remote_files[key].read_bytes() == blob
