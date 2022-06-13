from __future__ import annotations

from dataclasses import dataclass
import sys
from typing import Any, AsyncIterator, Optional, Sequence, cast

from anyio.abc import AsyncResource
from dandi.consts import known_instances
from dandi.dandiapi import RemoteAsset
from dandi.dandiapi import RemoteDandiset as SyncRemoteDandiset
from dandi.dandiapi import Version
import httpx

from .aioutil import arequest
from .consts import USER_AGENT

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class AsyncDandiClient(AsyncResource):
    session: httpx.AsyncClient

    @classmethod
    def for_dandi_instance(cls, instance: str) -> AsyncDandiClient:
        return cls(
            session=httpx.AsyncClient(
                base_url=known_instances[instance].api,
                headers={"User-Agent": USER_AGENT},
                follow_redirects=True,
            )
        )

    async def aclose(self) -> None:
        await self.session.aclose()

    async def get(self, path: str, **kwargs: Any) -> Any:
        return (await arequest(self.session, "GET", path, **kwargs)).json()

    async def paginate(
        self,
        path: str,
        page_size: Optional[int] = None,
        params: Optional[dict] = None,
        **kwargs: Any,
    ) -> AsyncIterator:
        """
        Paginate through the resources at the given path: GET the path, yield
        the values in the ``"results"`` key, and repeat with the URL in the
        ``"next"`` key until it is ``null``.
        """
        if page_size is not None:
            if params is None:
                params = {}
            params["page_size"] = page_size
        r = await self.get(path, params=params, **kwargs)
        while True:
            for item in r["results"]:
                yield item
            if r.get("next"):
                r = await self.get(r["next"])
            else:
                break

    async def aget_dandiset(
        self, dandiset_id: str, version_id: Optional[str] = None
    ) -> RemoteDandiset:
        data = await self.get(f"/dandisets/{dandiset_id}/")
        d = RemoteDandiset.from_data(self, data)
        if version_id is not None and version_id != d.version_id:
            if version_id == "draft":
                dv = d.for_version(d.draft_version)
            else:
                dv = d.for_version(await d.aget_version(version_id))
            assert isinstance(dv, RemoteDandiset)
            return dv
        return d

    async def get_dandisets(self) -> AsyncIterator[RemoteDandiset]:
        # Unlike the synchronous method, this always uses the draft version
        async with aclosing(self.paginate("/dandisets/")) as ait:
            async for data in ait:
                yield RemoteDandiset.from_data(self, data)

    async def get_dandisets_by_ids(
        self, dandiset_ids: Sequence[str]
    ) -> AsyncIterator[RemoteDandiset]:
        for did in dandiset_ids:
            data = await self.get(f"/dandisets/{did}/")
            yield RemoteDandiset.from_data(self, data)


class RemoteDandiset(SyncRemoteDandiset):
    def __init__(self, aclient: AsyncDandiClient, **kwargs: Any) -> None:
        super().__init__(client=None, **kwargs)
        self.aclient = aclient

    @classmethod
    def from_data(cls, aclient: AsyncDandiClient, data: dict) -> RemoteDandiset:
        # Unlike the synchronous method, this always uses the draft version
        return cls(
            aclient=aclient,
            identifier=data["identifier"],
            version=Version.parse_obj(data["draft_version"]),
            data=data,
        )

    def __repr__(self) -> str:
        return f"<Dandiset {self.identifier}/{self.version_id}>"

    def __str__(self) -> str:
        return f"Dandiset {self.identifier}/{self.version_id}"

    async def aget_raw_metadata(self) -> dict:
        return cast(dict, await self.aclient.get(self.version_api_path))

    async def aget_version(self, version_id: str) -> Version:
        return Version.parse_obj(
            self.aclient.get(f"/dandisets/{self.identifier}/versions/{version_id}/info")
        )

    async def aget_versions(self, include_draft: bool = True) -> AsyncIterator[Version]:
        async with aclosing(self.client.paginate(f"{self.api_path}versions/")) as ait:
            async for v in ait:
                if v["identifier"] != "draft" or include_draft:
                    yield Version.parse_obj(v)

    async def aget_assets(self) -> AsyncIterator[RemoteAsset]:
        async for item in self.aclient.paginate(
            f"{self.version_api_path}assets/", params={"order": "created"}
        ):
            metadata = await self.aget(f"/assets/{item['asset_id']}/")
            yield RemoteAsset.from_data(self, item, metadata)
