from __future__ import annotations

from dataclasses import InitVar, dataclass, field, replace
from typing import Any, Optional

from anyio.abc import AsyncResource
from ghrepo import GHRepo
import httpx
from humanize import naturalsize

from .adandi import RemoteDandiset
from .adataset import DatasetStats
from .aioutil import arequest
from .config import BackupConfig
from .consts import USER_AGENT
from .logging import PrefixedLogger, log
from .util import quantify


@dataclass
class Manager(AsyncResource):
    config: BackupConfig
    gh: Optional[GitHub]
    log: PrefixedLogger

    async def aclose(self) -> None:
        if self.gh is not None:
            await self.gh.aclose()

    def with_sublogger(self, prefix: str) -> Manager:
        return replace(self, log=self.log.sublogger(prefix))

    async def edit_github_repo(self, repo: GHRepo, **kwargs: Any) -> None:
        assert self.gh is not None
        await self.gh.edit_repo(repo, **kwargs)

    async def set_dandiset_description(
        self, dandiset: RemoteDandiset, stats: DatasetStats
    ) -> None:
        assert self.config.gh_org is not None
        assert self.gh is not None
        await self.gh.edit_repo(
            GHRepo(self.config.gh_org, dandiset.identifier),
            homepage=f"https://identifiers.org/DANDI:{dandiset.identifier}",
            description=await self.describe_dandiset(dandiset, stats),
        )

    async def describe_dandiset(
        self, dandiset: RemoteDandiset, stats: DatasetStats
    ) -> str:
        metadata = await dandiset.aget_raw_metadata()
        desc = dandiset.version.name
        contact = ", ".join(
            c["name"]
            for c in metadata.get("contributor", [])
            if "dandi:ContactPerson" in c.get("roleName", []) and "name" in c
        )
        if contact:
            desc = f"{contact}, {desc}"
        versions = 0
        async for v in dandiset.aget_versions(include_draft=False):
            versions += 1
        if versions:
            desc = f"{quantify(versions, 'release')}, {desc}"
        size = naturalsize(stats.size)
        return f"{quantify(stats.files, 'file')}, {size}, {desc}"

    async def set_zarr_description(self, zarr_id: str, stats: DatasetStats) -> None:
        assert self.config.zarr_gh_org is not None
        assert self.gh is not None
        size = naturalsize(stats.size)
        await self.gh.edit_repo(
            GHRepo(self.config.zarr_gh_org, zarr_id),
            description=f"{quantify(stats.files, 'file')}, {size}",
        )


@dataclass
class GitHub(AsyncResource):
    token: InitVar[str]
    client: httpx.AsyncClient = field(init=False)

    def __post_init__(self, token: str) -> None:
        self.client = httpx.AsyncClient(
            headers={"Authorization": f"token {token}", "User-Agent": USER_AGENT},
            follow_redirects=True,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def edit_repo(self, repo: GHRepo, **kwargs: Any) -> None:
        log.debug("Editing repository %s", repo)
        # Retry on 404's in case we're calling this right after
        # create_github_sibling(), when the repo may not yet exist
        await arequest(self.client, "PATCH", repo.api_url, json=kwargs, retry_on=[404])
