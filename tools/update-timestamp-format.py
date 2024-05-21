#!/usr/bin/env python3
# https://github.com/dandi/backups2datalad/issues/40
import json
from pathlib import Path
import re
import subprocess

from datalad.support.json_py import dump

DANDISETS_PATH = Path("/mnt/backup/dandi/dandisets")


def main() -> None:
    to_save = []
    for p in DANDISETS_PATH.iterdir():
        if p.is_dir() and re.fullmatch(r"[0-9]{6}", p.name):
            print(f"Processing {p.name} …")
            fpath = p / ".dandi" / "assets.json"
            try:
                with fpath.open() as fp:
                    data = json.load(fp)
            except FileNotFoundError:
                print("- No .dandi/assets.json; skipping")
                continue
            changed = False
            for asset in data:
                for field in ("created", "modified"):
                    try:
                        old_ts = asset[field]
                    except KeyError:
                        continue
                    new_ts = update_timestamp(old_ts)
                    if new_ts != old_ts:
                        asset[field] = new_ts
                        changed = True
            if changed:
                dump(data, fpath)
                print("- Committing …")
                subprocess.run(
                    [
                        "git",
                        "commit",
                        "-m",
                        (
                            "Update asset timestamp properties format in"
                            " .dandi/assets.json"
                        ),
                        ".dandi/assets.json",
                    ],
                    cwd=p,
                    check=True,
                )
                to_save.append(p.name)
            else:
                print("- No changes to .dandi/assets.json")
    if to_save:
        print("Saving superdataset …")
        subprocess.run(
            [
                "datalad",
                "save",
                "-d",
                ".",
                "-m",
                "Update asset timestamp properties format",
                *to_save,
            ],
            cwd=DANDISETS_PATH,
            check=True,
        )


def update_timestamp(ts: str) -> str:
    return re.sub(r"\+00:00$", "Z", ts)


if __name__ == "__main__":
    main()
