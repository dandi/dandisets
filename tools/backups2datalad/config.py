from __future__ import annotations

from enum import Enum
from functools import cached_property
from pathlib import Path
from re import Pattern

import anyio
from dandi.utils import yaml_dump, yaml_load
from pydantic import BaseModel, Field, model_validator

from .consts import DEFAULT_GIT_ANNEX_JOBS, DEFAULT_WORKERS, ZARR_LIMIT


class Remote(BaseModel):
    name: str
    type: str
    options: dict[str, str]


class ResourceConfig(BaseModel):
    path: Path
    github_org: str | None = None
    remote: Remote | None = None


class Mode(str, Enum):
    TIMESTAMP = "timestamp"
    VERIFY = "verify"
    FORCE = "force"

    def __str__(self) -> str:
        return self.value


class ZarrMode(str, Enum):
    TIMESTAMP = "timestamp"
    CHECKSUM = "checksum"
    FORCE = "force"
    ASSET_CHECKSUM = "asset-checksum"

    def __str__(self) -> str:
        return self.value


class BackupConfig(BaseModel):
    # Give everything a default so we can construct an "empty" config when no
    # config file is given
    dandi_instance: str = "dandi"
    s3bucket: str = "dandiarchive"
    content_url_regex: str = r"amazonaws.com/.*blobs/"
    dandisets: ResourceConfig = Field(
        default_factory=lambda: ResourceConfig(path="dandisets")
    )
    zarrs: ResourceConfig | None = None

    # Also settable via CLI options:
    backup_root: Path = Field(default_factory=Path)
    asset_filter: Pattern[str] | None = None
    jobs: int = DEFAULT_GIT_ANNEX_JOBS
    workers: int = DEFAULT_WORKERS
    force: str | None = None
    enable_tags: bool = True
    gc_assets: bool = False
    mode: Mode = Mode.TIMESTAMP
    zarr_mode: ZarrMode = ZarrMode.TIMESTAMP

    @model_validator(mode="after")
    def _validate(self) -> BackupConfig:
        gh_org = self.dandisets.github_org
        if self.zarrs is not None:
            zarr_gh_org = self.zarrs.github_org
        else:
            zarr_gh_org = None
        if (gh_org is None) != (zarr_gh_org is None):
            raise ValueError(
                "dandisets.github_org and zarrs.github_org must be either both"
                " set or both unset"
            )
        return self

    @classmethod
    def load_yaml(cls, filepath: Path) -> BackupConfig:
        with filepath.open() as fp:
            data = yaml_load(fp)
        return cls.model_validate(data)

    def dump_yaml(self, filepath: Path) -> None:
        filepath.write_text(yaml_dump(self.model_dump(mode="json", exclude_unset=True)))

    @property
    def dandiset_root(self) -> Path:
        return self.backup_root / self.dandisets.path

    @property
    def zarr_root(self) -> Path | None:
        if self.zarrs is not None:
            return self.backup_root / self.zarrs.path
        else:
            return None

    @property
    def gh_org(self) -> str | None:
        return self.dandisets.github_org

    @property
    def zarr_gh_org(self) -> str | None:
        if self.zarrs is not None:
            return self.zarrs.github_org
        else:
            return None

    @cached_property
    def zarr_limit(self) -> anyio.CapacityLimiter:
        return anyio.CapacityLimiter(ZARR_LIMIT)

    def match_asset(self, asset_path: str) -> bool:
        return self.asset_filter is None or bool(self.asset_filter.search(asset_path))
