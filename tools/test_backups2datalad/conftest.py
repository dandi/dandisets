from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import Any, Iterator, List, Optional, Union

from dandi.consts import dandiset_metadata_file
from dandi.dandiapi import DandiAPIClient, RemoteDandiset
from dandi.upload import upload
import pytest


@pytest.fixture(autouse=True)
def capture_all_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="backups2datalad")
    caplog.set_level(logging.DEBUG, logger="test_backups2datalad")


@pytest.fixture(scope="session")
def dandi_client() -> DandiAPIClient:
    api_token = os.environ["DANDI_API_KEY"]
    with DandiAPIClient.for_dandi_instance("dandi-staging", token=api_token) as client:
        yield client


@dataclass
class SampleDandiset:
    client: DandiAPIClient
    dspath: Path
    dandiset: RemoteDandiset
    dandiset_id: str

    def upload(
        self, paths: Optional[List[Union[str, Path]]] = None, **kwargs: Any
    ) -> None:
        upload(
            paths=paths or [self.dspath],
            dandi_instance="dandi-staging",
            devel_debug=True,
            allow_any_path=True,
            validation="skip",
            **kwargs,
        )


@pytest.fixture()
def text_dandiset(
    dandi_client: DandiAPIClient, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[SampleDandiset]:
    d = dandi_client.create_dandiset(
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
    dspath = tmp_path_factory.mktemp("text_dandiset")
    (dspath / dandiset_metadata_file).write_text(f"identifier: '{dandiset_id}'\n")
    (dspath / "file.txt").write_text("This is test text.\n")
    (dspath / "v0.txt").write_text("Version 0\n")
    (dspath / "subdir1").mkdir()
    (dspath / "subdir1" / "apple.txt").write_text("Apple\n")
    (dspath / "subdir2").mkdir()
    (dspath / "subdir2" / "banana.txt").write_text("Banana\n")
    (dspath / "subdir2" / "coconut.txt").write_text("Coconut\n")

    try:
        ds = SampleDandiset(
            client=dandi_client,
            dspath=dspath,
            dandiset=d,
            dandiset_id=d.identifier,
        )
        ds.upload()
        yield ds
    finally:
        for v in d.get_versions():
            if v.identifier != "draft":
                dandi_client.delete(f"{d.api_path}versions/{v.identifier}/")
        d.delete()
