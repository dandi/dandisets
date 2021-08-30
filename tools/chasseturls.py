#!/usr/bin/env python3
import re
import click
from datalad.api import Dataset


@click.command()
@click.argument("datasets", type=click.Path(file_okay=False, exists=True), nargs=-1)
def main(datasets):
    for ds in map(Dataset, datasets):
        ds.repo.always_commit = False
        changed = False
        for key, v in ds.repo.whereis(None, output="full", options=["--all"]).items():
            for url in v.get("00000000-0000-0000-0000-000000000001", {}).get("urls"):
                m = re.fullmatch(
                    r"https://(?P<domain>[^/]+)/api/dandisets/\d+/versions/"
                    r"draft/assets/(?P<asset_id>[^?/]+)/download/",
                    url,
                )
                if m:
                    new_url = (
                        f"https://{m['domain']}/api/assets/{m['asset_id']}/download/"
                    )
                    ds.repo.call_annex(["unregisterurl", key, url])
                    ds.repo.call_annex(["registerurl", key, new_url])
                    changed = True
        if changed:
            # just to trigger annex to commit its changes
            ds.repo.always_commit = True
            print(ds.repo.call_annex(["info"]))


if __name__ == "__main__":
    main()
