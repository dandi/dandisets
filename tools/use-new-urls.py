#!/usr/bin/env python

__requires__ = [
    "boto3",
    "click >= 7.0",
    "click-loglevel ~= 0.2",
    "dandi >= 0.14.0",
    "datalad",
]

import logging
from pathlib import Path
import sys
from urllib.parse import urlparse, urlunparse

import boto3
from botocore import UNSIGNED
from botocore.client import Config
import click
from click_loglevel import LogLevel
from dandi.dandiapi import DandiAPIClient
from datalad.api import Dataset

log = logging.getLogger(Path(sys.argv[0]).name)


@click.command()
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default="INFO",
    help="Set logging level",
    show_default=True,
)
@click.argument("datasets_path", type=click.Path(exists=True, file_okay=False))
@click.argument("dandisets", nargs=-1)
def main(datasets_path, dandisets, log_level):
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level,
        force=True,  # Override dandi's settings
    )
    URLUpdater(Path(datasets_path)).run(dandisets)


class URLUpdater:
    def __init__(self, datasets_path: Path):
        self.datasets_path = datasets_path
        self.dandi_client = DandiAPIClient("https://api.dandiarchive.org/api")
        self.s3client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    def run(self, dandisets=()):
        with self.dandi_client.session():
            for did in dandisets or self.get_dandiset_ids():
                dsdir = self.datasets_path / did
                log.info("Updating URLs for Dandiset %s", did)
                ds = Dataset(str(dsdir))
                self.update_dandiset_urls(did, ds)
                log.info("Pushing to sibling")
                ds.push(to="github")

    def update_dandiset_urls(self, dandiset_id, ds):
        if ds.repo.dirty:
            raise RuntimeError("Dirty repository; clean or save before running")
        ds.repo.always_commit = False
        for a in self.dandi_client.get_dandiset_assets(
            dandiset_id, "draft", include_metadata=False
        ):
            path = a["path"]
            log.info("Processing asset %s", path)
            if ds.repo.is_under_annex(path, batch=True):
                file_urls = set(ds.repo.get_urls(path, batch=True))
                bucket_url = self.get_file_bucket_url(
                    dandiset_id, "draft", a["asset_id"]
                )
                download_url = (
                    f"https://api.dandiarchive.org/api/dandisets/{dandiset_id}"
                    f"/versions/draft/assets/{a['asset_id']}/download/"
                )
                for url in [bucket_url, download_url]:
                    if url not in file_urls:
                        log.info("Adding URL %s to asset", url)
                        ds.repo.add_url_to_file(path, url, batch=True)
                for url in file_urls:
                    if "dandiarchive.s3.amazonaws.com/girder-assetstore/" in url:
                        log.info("Removing URL %s from asset", url)
                        ds.repo.rm_url(path, url)

            else:
                log.info("File is not managed by git annex; not updating URLs")
        log.info("Commiting changes")
        ds.save(message="Ran use-new-urls.py")

    def get_dandiset_ids(self):
        r = self.dandi_client.get("/dandisets/")
        while True:
            for d in r["results"]:
                yield d["identifier"]
            if r.get("next"):
                r = self.dandi_client.get(r.get("next"))
            else:
                break

    def get_file_bucket_url(self, dandiset_id, version_id, asset_id):
        r = self.dandi_client.send_request(
            "HEAD",
            f"/dandisets/{dandiset_id}/versions/{version_id}/assets/{asset_id}"
            "/download/",
            json_resp=False,
        )
        urlbits = urlparse(r.headers["Location"])
        s3meta = self.s3client.get_object(
            Bucket="dandiarchive", Key=urlbits.path.lstrip("/")
        )
        return urlunparse(urlbits._replace(query=f"versionId={s3meta['VersionId']}"))


if __name__ == "__main__":
    main()
