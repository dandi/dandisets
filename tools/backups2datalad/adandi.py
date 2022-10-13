from __future__ import annotations

from dataclasses import dataclass
import json
import sys
from time import time
from typing import Any, AsyncGenerator, Optional, Sequence, cast

import anyio
from anyio.abc import AsyncResource
from dandi.consts import known_instances
from dandi.dandiapi import DandiAPIClient, RemoteAsset
from dandi.dandiapi import RemoteDandiset as SyncRemoteDandiset
from dandi.dandiapi import RemoteZarrAsset, Version
import httpx

from .aioutil import arequest
from .consts import USER_AGENT
from .logging import log

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class AsyncDandiClient(AsyncResource):
    session: httpx.AsyncClient

    @classmethod
    def for_dandi_instance(
        cls, instance: str, token: Optional[str] = None
    ) -> AsyncDandiClient:
        headers = {"User-Agent": USER_AGENT}
        if token is not None:
            headers["Authorization"] = f"token {token}"
        return cls(
            session=httpx.AsyncClient(
                base_url=known_instances[instance].api,
                headers=headers,
                follow_redirects=True,
            )
        )

    async def aclose(self) -> None:
        await self.session.aclose()

    async def get(self, path: str, **kwargs: Any) -> Any:
        return (await arequest(self.session, "GET", path, **kwargs)).json()

    async def post(self, path: str, **kwargs: Any) -> Any:
        return (await arequest(self.session, "POST", path, **kwargs)).json()

    async def delete(self, path: str, **kwargs: Any) -> None:
        await arequest(self.session, "DELETE", path, **kwargs)

    async def paginate(
        self,
        path: str,
        page_size: Optional[int] = None,
        params: Optional[dict] = None,
        **kwargs: Any,
    ) -> AsyncGenerator:
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
                r = await self.get(r["next"], **kwargs)
            else:
                break

    async def get_dandiset(
        self, dandiset_id: str, version_id: Optional[str] = None
    ) -> RemoteDandiset:
        data = await self.get(f"/dandisets/{dandiset_id}/")
        d = RemoteDandiset.from_data(self, data)
        if version_id is not None and version_id != d.version_id:
            if version_id == "draft":
                return d.for_version(d.draft_version)
            else:
                return d.for_version(await d.aget_version(version_id))
        return d

    async def get_dandisets(self) -> AsyncGenerator[RemoteDandiset, None]:
        # Unlike the synchronous method, this always uses the draft version
        async with aclosing(self.paginate("/dandisets/")) as ait:
            async for data in ait:
                yield RemoteDandiset.from_data(self, data)

    async def get_dandisets_by_ids(
        self, dandiset_ids: Sequence[str]
    ) -> AsyncGenerator[RemoteDandiset, None]:
        for did in dandiset_ids:
            data = await self.get(f"/dandisets/{did}/")
            yield RemoteDandiset.from_data(self, data)

    async def create_dandiset(
        self, name: str, metadata: dict[str, Any]
    ) -> RemoteDandiset:
        return RemoteDandiset.from_data(
            self,
            await self.post("/dandisets/", json={"name": name, "metadata": metadata}),
        )


class RemoteDandiset(SyncRemoteDandiset):
    def __init__(self, aclient: AsyncDandiClient, **kwargs: Any) -> None:
        # We need to provide a DandiAPIClient so that pydantic doesn't error
        # when trying to construct asset instances
        nullclient = DandiAPIClient()
        # Ensure it doesn't accidentally make any requests:
        nullclient.session = None
        super().__init__(client=nullclient, **kwargs)
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

    def for_version(self, v: Version) -> RemoteDandiset:
        return type(self)(
            aclient=self.aclient,
            identifier=self.identifier,
            version=v,
            data=self._data,
        )

    async def aget_raw_metadata(self) -> dict:
        return cast(dict, await self.aclient.get(self.version_api_path))

    async def aget_version(self, version_id: str) -> Version:
        return Version.parse_obj(
            self.aclient.get(f"/dandisets/{self.identifier}/versions/{version_id}/info")
        )

    async def aget_versions(
        self, include_draft: bool = True, order: Optional[str] = None
    ) -> AsyncGenerator[Version, None]:
        async with aclosing(
            self.aclient.paginate(f"{self.api_path}versions/", params={"order": order})
        ) as ait:
            async for v in ait:
                if v["version"] != "draft" or include_draft:
                    yield Version.parse_obj(v)

    async def aget_assets(self) -> AsyncGenerator[RemoteAsset, None]:
        async with aclosing(
            self.aclient.paginate(
                f"{self.version_api_path}assets/",
                params={"order": "created", "metadata": "1", "page_size": "1000"},
                timeout=60,
            )
        ) as ait:
            async for item in ait:
                metadata = item.pop("metadata", None)
                yield RemoteAsset.from_data(self, item, metadata)

    async def aget_zarr_assets(self) -> AsyncGenerator[RemoteZarrAsset, None]:
        async with aclosing(self.aget_assets()) as ait:
            async for asset in ait:
                if isinstance(asset, RemoteZarrAsset):
                    yield asset

    async def aget_asset(self, asset_id: str) -> RemoteAsset:
        info = await self.aclient.get(f"{self.version_api_path}assets/{asset_id}/info/")
        metadata = info.pop("metadata", None)
        return RemoteAsset.from_data(self, info, metadata)

    async def aget_asset_by_path(self, path: str) -> RemoteAsset:
        async with aclosing(
            self.aclient.paginate(
                f"{self.version_api_path}assets/",
                params={"path": path, "metadata": "1", "page_size": "1000"},
            )
        ) as ait:
            async for item in ait:
                if item["path"] == path:
                    metadata = item.pop("metadata", None)
                    return RemoteAsset.from_data(self, item, metadata)
        raise ValueError(f"Asset not found: {path}")

    async def await_until_valid(self, max_time: float = 120) -> None:
        log.debug("Waiting for Dandiset %s to complete validation ...", self.identifier)
        start = time()
        while time() - start < max_time:
            r = await self.aclient.get(f"{self.version_api_path}info/")
            if r["status"] == "Valid":
                return
            await anyio.sleep(0.5)
        about = {
            "asset_validation_errors": r.get("asset_validation_errors"),
            "version_validation_errors": r.get("version_validation_errors"),
        }
        raise ValueError(
            f"Dandiset {self.identifier} is {r['status']}:"
            f" {json.dumps(about, indent=4)}"
        )

    async def apublish(self, max_time: float = 120) -> RemoteDandiset:
        draft_api_path = f"/dandisets/{self.identifier}/versions/draft/"
        await arequest(self.aclient.session, "POST", f"{draft_api_path}publish/")
        log.debug(
            "Waiting for Dandiset %s to complete publication ...", self.identifier
        )
        start = time()
        while time() - start < max_time:
            v = await self.aclient.get(f"{draft_api_path}info/")
            if v["status"] == "Published":
                break
            await anyio.sleep(0.5)
        else:
            raise ValueError(f"Dandiset {self.identifier} did not publish in time")
        async for v in self.aget_versions(order="-created"):
            return self.for_version(v)
        raise AssertionError(
            f"No published versions found for Dandiset {self.identifier}"
        )

    async def adelete(self) -> None:
        await self.aclient.delete(self.api_path)
