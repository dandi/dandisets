#!/usr/bin/env python3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
import json
import logging
from operator import attrgetter
from pathlib import Path, PurePosixPath
import re
import subprocess
from typing import Any, Dict, Iterator, List, Optional, Set, TextIO, Tuple, Union, cast

import click
from datalad.support.annexrepo import AnnexRepo
from dateutil.parser import parse
from humanize import naturalsize
import ruamel.yaml

log = logging.getLogger("get-upload-stats")

IGNORED = {".dandi", ".datalad", ".gitattributes", "dandiset.yaml"}


@dataclass
class AssetInfo:
    path: str
    size: int
    key: str
    subject: Optional[str]


@dataclass
class CommitInfo:
    committish: str
    short_hash: str
    created: datetime


@dataclass
class CommitDelta:
    first: CommitInfo
    second: CommitInfo
    assets_added: int
    assets_added_size: int
    assets_removed: int
    assets_removed_size: int
    duplicates_delta: int
    duplicates_delta_size: int
    duplicates_remaining: int
    subjects_added: int

    def __bool__(self) -> bool:
        return any(
            getattr(self, field) != 0
            for field in [
                "assets_added",
                "assets_added_size",
                "assets_removed",
                "assets_removed_size",
                "duplicates_delta",
                "duplicates_delta_size",
                # NOT "duplicates_remaining",
                "subjects_added",
            ]
        )

    def for_json(self) -> dict:
        return {
            "first": {
                "committish": self.first.short_hash,
                "created": self.first.created.isoformat(),
            },
            "second": {
                "committish": self.second.short_hash,
                "created": self.second.created.isoformat(),
            },
            "assets": {
                "distinct": {
                    "added": self.assets_added,
                    "added_size": self.assets_added_size,
                    "removed": self.assets_removed,
                    "removed_size": self.assets_removed_size,
                },
                "duplicates": {
                    "delta": self.duplicates_delta,
                    "delta_size": self.duplicates_delta_size,
                    "remaining": self.duplicates_remaining,
                },
            },
            "subjects": {
                "added": self.subjects_added,
            },
        }

    def to_markdown(self) -> str:
        s = f"## Changes from {self.first.short_hash} to {self.second.short_hash}\n\n"
        s += (
            f"* Assets added: {self.assets_added}"
            f" ({naturalsize(self.assets_added_size)})\n"
        )
        s += (
            f"* Assets removed: {self.assets_removed}"
            f" ({naturalsize(self.assets_removed_size)})\n"
        )
        s += (
            f"* Duplicates delta: {self.duplicates_delta}"
            f" ({naturalsize(self.duplicates_delta_size)})\n"
        )
        s += f"* Duplicates remaining: {self.duplicates_remaining}\n"
        s += f"* Subjects added: {self.subjects_added}\n"
        return s


@dataclass
class Report:
    from_dt: Optional[datetime]
    to_dt: Optional[datetime]
    commit_delta: CommitDelta
    published_versions: int
    since_latest: Optional[CommitDelta]

    def for_json(self) -> dict:
        return {
            "from_dt": self.from_dt.isoformat() if self.from_dt is not None else None,
            "to_dt": self.to_dt.isoformat() if self.to_dt is not None else None,
            "commit_delta": self.commit_delta.for_json(),
            "published_versions": self.published_versions,
            "since_latest": self.since_latest.for_json()
            if self.since_latest is not None
            else None,
        }

    def to_markdown(self) -> str:
        s = ""
        if self.from_dt is not None and self.to_dt is not None:
            s += f"# Changes from {self.from_dt} to {self.to_dt}\n\n"
        elif self.from_dt is not None:
            s += f"# Changes since {self.from_dt}\n\n"
        elif self.to_dt is not None:
            s += f"# Changes up to {self.to_dt}\n\n"
        else:
            s += "# Changes\n\n"
        s += f"**Versions published:** {self.published_versions}\n\n"
        if self.since_latest is not None and not self.since_latest:
            s += (
                f"No changes since version {self.since_latest.first.short_hash}"
                f" published on {self.since_latest.first.created}\n\n"
            )
        s += self.commit_delta.to_markdown()
        if self.since_latest is not None and self.since_latest:
            s += "\n\n" + self.since_latest.to_markdown()
        return s


@dataclass
class DandiDataSet:
    path: Path

    def readgit(self, *args: Union[str, Path], **kwargs: Any) -> str:
        return cast(
            str,
            subprocess.check_output(["git", *args], cwd=self.path, text=True, **kwargs),
        ).strip()

    def get_first_and_last_commit(
        self, from_dt: Optional[datetime], to_dt: Optional[datetime]
    ) -> Tuple[CommitInfo, CommitInfo]:
        # --before orders by commit date, not author date, so we need to filter
        # commits ourselves.
        cmtlines = self.readgit(
            "log",
            r"--grep=\[backups2datalad\]",
            r"--grep=Ran backups2datalad\.py",
            "--format=%H %h %aI %p",
        ).splitlines()
        commits: List[CommitInfo] = []
        warned_nonlinear = False
        for cmt in cmtlines:
            committish, short_hash, ts, *parents = cmt.strip().split()
            if len(parents) > 1 and not warned_nonlinear:
                log.warning("Commits in given timeframe are nonlinear")
                warned_nonlinear = True
            commits.append(
                CommitInfo(
                    committish=committish,
                    short_hash=short_hash,
                    created=datetime.fromisoformat(ts),
                )
            )
        commits.sort(key=attrgetter("created"))
        commits = filter_commits(commits, from_dt, to_dt)
        if not commits:
            raise ValueError("No commits in given timeframe")
        elif len(commits) == 1:
            raise ValueError("Only one commit in given timeframe")
        return (commits[0], commits[-1])

    def get_tags(
        self, from_dt: Optional[datetime], to_dt: Optional[datetime]
    ) -> List[CommitInfo]:
        # Tags are returned in ascending order of creation
        taglines = self.readgit(
            "tag",
            "-l",
            "--sort=creatordate",
            "--format=%(creatordate:iso-strict) %(refname:strip=2)",
        ).splitlines()
        tags: List[CommitInfo] = []
        for tl in taglines:
            ts, _, tag = tl.partition(" ")
            tags.append(
                CommitInfo(
                    committish=tag, short_hash=tag, created=datetime.fromisoformat(ts)
                )
            )
        return filter_commits(tags, from_dt, to_dt)

    def get_assets(self, commit: CommitInfo) -> Iterator[AssetInfo]:
        repo = AnnexRepo(str(self.path))
        for p, info in repo.get_content_annexinfo(ref=commit.committish).items():
            relpath = p.relative_to(self.path)
            if info.get("type") == "file" and relpath.parts[0] not in IGNORED:
                path = str(PurePosixPath(relpath))
                subject: Optional[str]
                if m := re.fullmatch(r"sub-([^/]+)/[^/]+\.nwb", path):
                    subject = m.group(1)
                else:
                    subject = None
                if info.get("backend"):
                    # Annexed
                    yield AssetInfo(
                        path=path,
                        size=info["bytesize"],
                        key=info["key"],
                        subject=subject,
                    )
                else:
                    # Not annexed
                    yield AssetInfo(
                        path=path,
                        size=int(self.readgit("cat-file", "-s", info["gitshasum"])),
                        key=info["gitshasum"],
                        subject=subject,
                    )

    def cmp_commit_assets(
        self, commit1: CommitInfo, commit2: CommitInfo
    ) -> CommitDelta:
        asset_sizes: Dict[str, int] = {}
        key_qtys: List[Dict[str, int]] = []
        subjects_sets: List[Set[str]] = []
        for cmt in [commit1, commit2]:
            keys: Dict[str, int] = Counter()
            subjects: Set[str] = set()
            for asset in self.get_assets(cmt):
                asset_sizes[asset.key] = asset.size
                keys[asset.key] += 1
                if asset.subject is not None:
                    subjects.add(asset.subject)
            key_qtys.append(keys)
            subjects_sets.append(subjects)
        keys1, keys2 = key_qtys
        added_keys = keys2.keys() - keys1.keys()
        removed_keys = keys1.keys() - keys2.keys()
        duplicates1 = Counter({k: n - 1 for k, n in keys1.items() if n > 1})
        duplicates2 = Counter({k: n - 1 for k, n in keys2.items() if n > 1})
        subjects1, subjects2 = subjects_sets
        return CommitDelta(
            first=commit1,
            second=commit2,
            assets_added=len(added_keys),
            assets_added_size=sum(asset_sizes[k] for k in added_keys),
            assets_removed=len(removed_keys),
            assets_removed_size=sum(asset_sizes[k] for k in removed_keys),
            duplicates_delta=sum(duplicates2.values()) - sum(duplicates1.values()),
            duplicates_delta_size=sum(
                asset_sizes[k] * n for k, n in duplicates2.items()
            )
            - sum(asset_sizes[k] * n for k, n in duplicates1.items()),
            duplicates_remaining=sum(duplicates2.values()),
            subjects_added=len(subjects2 - subjects1),
        )


@click.command()
@click.option(
    "--from",
    "from_dt",
    type=parse,
    metavar="DATETIME",
    help=(
        "The lower bound (inclusive) on author dates of commits to consider;"
        " defaults to the beginning of time"
    ),
)
@click.option(
    "--to",
    "to_dt",
    type=parse,
    metavar="DATETIME",
    help=(
        "The upper bound (exclusive) on author dates of commits to consider;"
        " defaults to the end of time"
    ),
)
@click.option("-o", "--outfile", type=click.File("w"), default="-")
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["markdown", "json", "yaml"]),
    default="markdown",
)
@click.argument(
    "dandiset",
    type=click.Path(exists=True, file_okay=False, resolve_path=True, path_type=Path),
)
def main(
    dandiset: Path,
    from_dt: Optional[datetime],
    to_dt: Optional[datetime],
    outfile: TextIO,
    fmt: str,
) -> None:
    """
    Summarize the net changes in assets in a `backups2datalad.py` dataset
    across a given time range.

    The timestamps passed to the --from and --to options can be in any format
    supported by python-dateutil's `dateutil.parser.parse()` function.
    Recommended formats include:

    \b
        * 2021-09-22T14:21:43-04:00
        * 2021-09-22 14:21:43 -0400
        * 2021-09-22  (Time defaults to midnight)
        * Wed, 22 Sep 2021 14:21:43 -0400
        * Wed Sep 22 14:21:43 EDT 2021  (when the given timezone abbreviation
          is either "UTC" or for the local system timezone)

    If a timestamp lacks timezone information, it is assumed to be in the local
    system timezone.
    """
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=logging.INFO,
    )
    if from_dt is not None and from_dt.tzinfo is None:
        from_dt = from_dt.astimezone()
    if to_dt is not None and to_dt.tzinfo is None:
        to_dt = to_dt.astimezone()
    dd = DandiDataSet(dandiset)
    commit1, commit2 = dd.get_first_and_last_commit(from_dt, to_dt)
    commit_delta = dd.cmp_commit_assets(commit1, commit2)
    tags = dd.get_tags(from_dt, to_dt)
    since_latest: Optional[CommitDelta]
    if tags:
        since_latest = dd.cmp_commit_assets(tags[-1], commit2)
    else:
        since_latest = None
    report = Report(
        from_dt=from_dt,
        to_dt=to_dt,
        commit_delta=commit_delta,
        published_versions=len(tags),
        since_latest=since_latest,
    )
    if fmt == "json":
        print(json.dumps(report.for_json(), indent=4), file=outfile)
    elif fmt == "yaml":
        yaml = ruamel.yaml.YAML(typ="safe")  # type: ignore[attr-defined]
        yaml.default_flow_style = False
        yaml.dump(report.for_json(), outfile)
    elif fmt == "markdown":
        print(report.to_markdown(), file=outfile)


def filter_commits(
    commits: List[CommitInfo], from_dt: Optional[datetime], to_dt: Optional[datetime]
) -> List[CommitInfo]:
    # `commits` must be sorted by `created` in ascending order
    if from_dt is not None:
        while commits and commits[0].created < from_dt:
            commits.pop(0)
    if to_dt is not None:
        while commits and commits[-1].created >= to_dt:
            commits.pop(-1)
    return commits


if __name__ == "__main__":
    main()
