from __future__ import annotations

from dataclasses import dataclass
from functools import partial
import logging
import os
from pathlib import Path
import sys
from typing import Any, AsyncIterator, Optional

import anyio
from dandi.consts import dandiset_metadata_file
from dandi.upload import upload
import pytest

from backups2datalad.adandi import AsyncDandiClient, RemoteDandiset

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def capture_all_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(5, logger="backups2datalad")
    caplog.set_level(logging.DEBUG, logger="test_backups2datalad")


@pytest.fixture
async def dandi_client() -> AsyncIterator[AsyncDandiClient]:
    api_token = os.environ["DANDI_API_KEY"]
    async with AsyncDandiClient.for_dandi_instance(
        "dandi-staging", token=api_token
    ) as client:
        yield client


@dataclass
class SampleDandiset:
    client: AsyncDandiClient
    dspath: Path
    dandiset: RemoteDandiset
    dandiset_id: str

    async def upload(
        self, paths: Optional[list[str | Path]] = None, **kwargs: Any
    ) -> None:
        await anyio.to_thread.run_sync(
            partial(
                upload,
                paths=paths or [self.dspath],
                dandi_instance="dandi-staging",
                devel_debug=True,
                allow_any_path=True,
                validation="skip",
                **kwargs,
            )
        )


@pytest.fixture()
async def new_dandiset(
    dandi_client: AsyncDandiClient, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[SampleDandiset]:
    d = await dandi_client.create_dandiset(
        "Dandiset for testing backups2datalad",
        {
            "name": "Dandiset for testing backups2datalad",
            "description": "A test text Dandiset",
            "contributor": [
                {
                    "schemaKey": "Person",
                    "name": "Wodder, John",
                    "roleName": ["dcite:Author", "dcite:ContactPerson"],
                }
            ],
            "license": ["spdx:CC0-1.0"],
        },
    )
    dandiset_id = d.identifier
    dspath = tmp_path_factory.mktemp("new_dandiset")
    (dspath / dandiset_metadata_file).write_text(f"identifier: '{dandiset_id}'\n")
    ds = SampleDandiset(
        client=dandi_client,
        dspath=dspath,
        dandiset=d,
        dandiset_id=d.identifier,
    )
    try:
        yield ds
    finally:
        async with aclosing(d.aget_versions(include_draft=False)) as vit:
            async for v in vit:
                await dandi_client.delete(f"{d.api_path}versions/{v.identifier}/")
        await d.adelete()


@pytest.fixture()
async def text_dandiset(new_dandiset: SampleDandiset) -> SampleDandiset:
    (new_dandiset.dspath / "file.txt").write_text("This is test text.\n")
    (new_dandiset.dspath / "v0.txt").write_text("Version 0\n")
    (new_dandiset.dspath / "subdir1").mkdir()
    (new_dandiset.dspath / "subdir1" / "apple.txt").write_text("Apple\n")
    (new_dandiset.dspath / "subdir2").mkdir()
    (new_dandiset.dspath / "subdir2" / "banana.txt").write_text("Banana\n")
    (new_dandiset.dspath / "subdir2" / "coconut.txt").write_text("Coconut\n")
    await new_dandiset.upload()
    return new_dandiset
