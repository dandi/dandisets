from __future__ import annotations

from enum import Enum
from functools import cached_property
import json
from pathlib import Path
from re import Pattern
from typing import Any

import anyio
from dandi.utils import yaml_dump, yaml_load
from pydantic import BaseModel, Field, root_validator

from .consts import DEFAULT_GIT_ANNEX_JOBS, DEFAULT_WORKERS, ZARR_LIMIT


class Remote(BaseModel):
    name: str
    type: str
    # Needs to be `typing.Dict` so that pydantic will support it under 3.8
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
    # <https://github.com/samuelcolvin/pydantic/issues/2636>
    asset_filter: Pattern | None = None
    jobs: int = DEFAULT_GIT_ANNEX_JOBS
    workers: int = DEFAULT_WORKERS
    force: str | None = None
    enable_tags: bool = True
    gc_assets: bool = False
    mode: Mode = Mode.TIMESTAMP
    zarr_mode: ZarrMode = ZarrMode.TIMESTAMP

    class Config:
        # <https://github.com/samuelcolvin/pydantic/issues/1241>
        arbitrary_types_allowed = True
        keep_untouched = (cached_property,)

    @root_validator
    def _validate(cls, values: dict[str, Any]) -> dict[str, Any]:  # noqa: B902, U100
        gh_org = values["dandisets"].github_org
        zcfg: ResourceConfig | None
        if (zcfg := values["zarrs"]) is not None:
            zarr_gh_org = zcfg.github_org
        else:
            zarr_gh_org = None
        if (gh_org is None) != (zarr_gh_org is None):
            raise ValueError(
                "dandisets.github_org and zarrs.github_org must be either both"
                " set or both unset"
            )
        return values

    @classmethod
    def load_yaml(cls, filepath: Path) -> BackupConfig:
        with filepath.open("r") as fp:
            data = yaml_load(fp)
        return cls.parse_obj(data)

    def dump_yaml(self, filepath: Path) -> None:
        filepath.write_text(yaml_dump(json.loads(self.json(exclude_unset=True))))

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
