from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
import json
import re
import sys
from time import time
from typing import Any, AsyncGenerator, Dict, Optional, Sequence, Type

import anyio
from anyio.abc import AsyncResource
from dandi.consts import known_instances
from dandi.dandiapi import AssetType, DandiAPIClient
from dandi.dandiapi import RemoteDandiset as SyncRemoteDandiset
from dandi.dandiapi import Version
from dandi.exceptions import NotFoundError
from dandischema.models import DigestType
import httpx
from pydantic import BaseModel, Field

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
    api_url: str

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
            ),
            api_url=known_instances[instance].api,
        )

    async def aclose(self) -> None:
        await self.session.aclose()

    def get_url(self, path: str) -> str:
        if path.lower().startswith(("http://", "https://")):
            return path
        else:
            return self.api_url.rstrip("/") + "/" + path.lstrip("/")

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
        md = await self.aclient.get(self.version_api_path)
        assert isinstance(md, dict)
        return md

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
                yield RemoteAsset.from_data(self, item)

    async def aget_zarr_assets(self) -> AsyncGenerator[RemoteZarrAsset, None]:
        async with aclosing(self.aget_assets()) as ait:
            async for asset in ait:
                if isinstance(asset, RemoteZarrAsset):
                    yield asset

    async def aget_asset(self, asset_id: str) -> RemoteAsset:
        info = await self.aclient.get(f"{self.version_api_path}assets/{asset_id}/info/")
        return RemoteAsset.from_data(self, info)

    async def aget_asset_by_path(self, path: str) -> RemoteAsset:
        async with aclosing(
            self.aclient.paginate(
                f"{self.version_api_path}assets/",
                params={"path": path, "metadata": "1", "page_size": "1000"},
            )
        ) as ait:
            async for item in ait:
                if item["path"] == path:
                    return RemoteAsset.from_data(self, item)
        raise ValueError(f"Asset not found: {path}")

    async def await_until_valid(self, max_time: float = 120) -> None:
        log.debug("Waiting for Dandiset %s to complete validation ...", self.identifier)
        start = time()
        while time() - start < max_time:
            r = await self.aclient.get(f"{self.version_api_path}info/")
            if r["status"] == "Valid" and not r.get("asset_validation_errors"):
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


class RemoteAsset(BaseModel):
    """
    A minimal, asynchronous version of `dandi.dandiapi.RemoteAsset`, which it
    differs from in the following ways:

    - `refetch()` method added
    - `aclient` property added
    - `get_digest()` and `get_raw_digest()` methods combined into
      `get_digest_value()`
    - No `BaseRemoteAsset` base class, as backups2datalad doesn't operate on
      Dandiset-less asset URLs
    - `dandiset` attribute added
    - `dandiset_id` and `version_id` attributes are now properties
    - `_metadata` attribute renamed to `metadata`
    - The `metadata` attribute is now always set
    - `from_data()` now expects ``metadata`` to be a field of ``data`` instead
      of a separate argument
    - The following methods & properties have been removed:

      - `get_metadata()`
      - `get_raw_metadata()` (Use the `metadata` attribute instead)
      - `get_download_file_iter()`
      - `download()`
      - `set_metadata()`
      - `set_raw_metadata()`
      - `rename()`
      - `delete()`
      - `api_url`
      - `download_url`

    - `get_content_url()` does not take ``follow_redirects`` or ``strip_query``
      parameters
    """

    dandiset: RemoteDandiset
    #: The asset identifier
    identifier: str = Field(alias="asset_id")
    #: The asset's (forward-slash-separated) path
    path: str
    #: The size of the asset in bytes
    size: int
    #: The date at which the asset was created
    created: datetime
    #: The date at which the asset was last modified
    modified: datetime
    #: The asset's raw metadata
    metadata: Dict[str, Any]

    class Config:
        """Configuration for pydantic"""

        # To use the non-pydantic RemoteDandiset as an attribute and to be able
        # to create instances with RemoteAsset(identifier=...) instead of
        # RemoteAsset(alias_id=...)
        allow_population_by_field_name = True
        arbitrary_types_allowed = True

    @classmethod
    def from_data(cls, dandiset: RemoteDandiset, data: Dict[str, Any]) -> RemoteAsset:
        """
        Construct a `RemoteAsset` instance from a `RemoteDandiset` and a `dict`
        of raw data in the same format as returned by the API's pagination
        endpoints (including metadata).
        """
        klass: Type[RemoteAsset]
        if data.get("blob") is not None:
            klass = RemoteBlobAsset
            if data.pop("zarr", None) is not None:
                raise ValueError("Asset data contains both `blob` and `zarr`'")
        elif data.get("zarr") is not None:
            klass = RemoteZarrAsset
            if data.pop("blob", None) is not None:
                raise ValueError("Asset data contains both `blob` and `zarr`'")
        else:
            raise ValueError("Asset data contains neither `blob` nor `zarr`")
        return klass(dandiset=dandiset, **data)  # type: ignore[call-arg]

    @property
    def aclient(self) -> AsyncDandiClient:
        return self.dandiset.aclient

    @property
    def dandiset_id(self) -> str:
        did = self.dandiset.identifier
        assert isinstance(did, str)
        return did

    @property
    def version_id(self) -> str:
        vid = self.dandiset.version_id
        assert isinstance(vid, str)
        return vid

    def json_dict(self) -> dict[str, Any]:
        data = json.loads(self.json(exclude={"aclient", "dandiset"}, by_alias=True))
        assert isinstance(data, dict)
        return data

    @property
    def api_path(self) -> str:
        """
        The path (relative to the base endpoint for a Dandi Archive API) at
        which API requests for interacting with the asset itself are made
        """
        return (
            f"/dandisets/{self.dandiset_id}/versions/{self.version_id}/assets"
            f"/{self.identifier}/"
        )

    @property
    def base_download_url(self) -> str:
        """
        The URL from which the asset can be downloaded, sans any Dandiset
        identifiers
        """
        return self.aclient.get_url(f"/assets/{self.identifier}/download/")

    def get_digest_value(self, algorithm: Optional[DigestType] = None) -> Optional[str]:
        if algorithm is None:
            algorithm = self.digest_type
        try:
            val = self.metadata["digest"][algorithm.value]
        except KeyError:
            raise NotFoundError(f"No {algorithm.value} digest found in metadata")
        assert val is None or isinstance(val, str)
        return val

    def get_content_url(self, regex: str = r".*") -> str:
        """
        Returns a URL for downloading the asset, found by inspecting the
        metadata; specifically, returns the first ``contentUrl`` that matches
        ``regex``.  Raises `NotFoundError` if the metadata does not contain a
        matching URL.
        """
        for url in self.metadata.get("contentUrl", []):
            assert isinstance(url, str)
            if re.search(regex, url):
                return url
        raise NotFoundError(
            "No matching URL found in asset's contentUrl metadata field"
        )

    async def refetch(self) -> RemoteAsset:
        # Query the Dandiset-free asset endpoint in case the asset's been
        # deleted from the Dandiset since we started running
        info = await self.aclient.get(f"/assets/{self.identifier}/info/")
        return RemoteAsset.from_data(self.dandiset, info)

    @property
    @abstractmethod
    def asset_type(self) -> AssetType:
        """The type of the asset's underlying data"""
        ...

    @property
    def digest_type(self) -> DigestType:
        """
        The primary digest algorithm used by Dandi Archive for the asset,
        determined based on its underlying data: dandi-etag for blob resources,
        dandi-zarr-checksum for Zarr resources
        """
        if self.asset_type is AssetType.ZARR:
            return DigestType.dandi_zarr_checksum
        else:
            return DigestType.dandi_etag


class RemoteBlobAsset(RemoteAsset):
    """A `RemoteAsset` whose actual data is a blob resource"""

    #: The ID of the underlying blob resource
    blob: str

    @property
    def asset_type(self) -> AssetType:
        """The type of the asset's underlying data"""
        return AssetType.BLOB


class RemoteZarrAsset(RemoteAsset):
    """A `RemoteAsset` whose actual data is a Zarr resource"""

    #: The ID of the underlying Zarr resource
    zarr: str

    @property
    def asset_type(self) -> AssetType:
        """The type of the asset's underlying data"""
        return AssetType.ZARR
