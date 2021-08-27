#!/usr/bin/python3
import re
import click
from datalad.api import Dataset


@click.command()
@click.argument("datasets", type=click.Path(file_okay=False, exists=True), nargs=-1)
def main(datasets):
    for ds in map(Dataset, datasets):
        ds.repo.always_commit = False
        changed = False
        for path in ds.repo.get_annexed_files():
            for url in ds.repo.get_urls(path):
                m = re.fullmatch(
                    r"https://(?P<domain>[^/]+)/api/dandisets/\d+/versions/"
                    r"draft/assets/(?P<asset_id>[^?/]+)/download/",
                    url,
                )
                if m:
                    new_url = (
                        f"https://{m['domain']}/api/assets/{m['asset_id']}/download/"
                    )
                    ds.repo.rm_url(path, url)
                    ds.repo.add_url_to_file(path, new_url, batch=True)
                    changed = True
        if changed:
            ds.save(
                message="Change Dandiset-specific asset URLs to top-level /assets/ URLs"
            )


if __name__ == "__main__":
    main()
