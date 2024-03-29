#!/usr/bin/env python3
from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from datetime import datetime, time, timezone
import json
import logging
from operator import attrgetter
from pathlib import Path, PurePosixPath
import re
import subprocess
from typing import Annotated, Any, TextIO

import click
from datalad.support.annexrepo import AnnexRepo
from dateutil.parser import parse
from humanize import naturalsize
from pydantic import BaseModel
from pydantic.functional_serializers import PlainSerializer
from ruamel.yaml import YAML  # type: ignore[attr-defined]

log = logging.getLogger("get-upload-stats")

IGNORED = {".dandi", ".datalad", ".gitattributes", "dandiset.yaml"}


class AddedRemoved(BaseModel):
    added: int
    removed: int

    def __bool__(self) -> bool:
        return bool(self.added or self.removed)


class SetAddedRemoved(BaseModel):
    added: set[str]
    removed: set[str]

    def __bool__(self) -> bool:
        return bool(self.added or self.removed)


class AssetInfo(BaseModel):
    path: str
    size: int
    key: str
    subject: str | None


class CommitInfo(BaseModel):
    committish: str
    short_id: str
    created: datetime


SortedSet = Annotated[set[str], PlainSerializer(lambda s: sorted(s))]


class MetadataSummary(BaseModel):
    specimens: SortedSet | None
    species: SortedSet | None
    modalities: SortedSet | None
    techniques: SortedSet | None
    anatomies: SortedSet

    @classmethod
    def from_metadata(
        cls, dandiset_metadata: dict, asset_metadata: list[dict] | None
    ) -> MetadataSummary:
        specimens: set[str] | None
        if asset_metadata is not None:
            specimens = {sp for am in asset_metadata for sp in cls.get_specimens(am)}
        else:
            specimens = None
        species: set[str] | None
        techniques: set[str] | None
        modalities: set[str] | None
        try:
            summary = dandiset_metadata["assetsSummary"]
        except KeyError:
            species = None
            techniques = None
            modalities = None
        else:
            species = {sp["name"] for sp in (summary.get("species") or [])}
            techniques = {
                tq["name"] for tq in (summary.get("measurementTechnique") or [])
            }
            modalities = {md["name"] for md in (summary.get("approach") or [])}
        anatomies = {
            ab["name"]
            for ab in dandiset_metadata.get("about") or []
            if ab.get("schemaKey") == "Anatomy"
        }
        return cls(
            specimens=specimens,
            species=species,
            modalities=modalities,
            techniques=techniques,
            anatomies=anatomies,
        )

    @staticmethod
    def get_specimens(d: dict) -> Iterator[str]:
        for biosample in d.get("wasDerivedFrom") or []:
            yield biosample["sampleType"]["name"]
            yield from MetadataSummary.get_specimens(biosample)

    def to_markdown(self) -> str:
        s = ""
        for label, field in [
            ("Specimen Types", "specimens"),
            ("Species", "species"),
            ("Modalities", "modalities"),
            ("Techniques", "techniques"),
            ("Anatomical Structures", "anatomies"),
        ]:
            s += f"* **{label}:** "
            v: set[str] | None = getattr(self, field)
            if v is None:
                s += "[data not available]"
            elif not v:
                s += "[none]"
            else:
                s += ", ".join(sorted(v))
            s += "\n"
        return s


class MetadataDiff(BaseModel):
    specimens: SetAddedRemoved | None
    species: SetAddedRemoved | None
    modalities: SetAddedRemoved | None
    techniques: SetAddedRemoved | None
    anatomies: SetAddedRemoved

    @classmethod
    def compare(cls, first: MetadataSummary, second: MetadataSummary) -> "MetadataDiff":
        def cmp(one: set[str] | None, two: set[str] | None) -> SetAddedRemoved | None:
            if one is None:
                one = set()
            if two is None:
                return None
            return SetAddedRemoved(added=two - one, removed=one - two)

        return cls(
            **{
                k: cmp(getattr(first, k), getattr(second, k))
                for k in cls.model_fields.keys()
            }
        )

    def __bool__(self) -> bool:
        # Don't iterate over self.dict().values(), as that converts models in
        # subfields to dicts
        return bool(any(v for _, v in self))

    def to_markdown(self) -> str:
        if not self:
            return "No change\n"
        s = ""
        for label, field in [
            ("Specimen Types", "specimens"),
            ("Species", "species"),
            ("Modalities", "modalities"),
            ("Techniques", "techniques"),
            ("Anatomical Structures", "anatomies"),
        ]:
            s += f"* **{label}:**"
            v: SetAddedRemoved | None = getattr(self, field)
            if v is None:
                s += " [data not available]\n"
            elif not v:
                s += " no change\n"
            else:
                s += "\n"
                s += "    * Added: "
                if v.added:
                    s += ", ".join(sorted(v.added))
                else:
                    s += "\u2014"
                s += "\n"
                s += "    * Removed: "
                if v.removed:
                    s += ", ".join(sorted(v.removed))
                else:
                    s += "\u2014"
                s += "\n"
        return s


class UniqueAssetsDelta(BaseModel):
    by_qty: AddedRemoved
    by_bytes: AddedRemoved

    def __bool__(self) -> bool:
        return bool(self.by_qty) or bool(self.by_bytes)


class DuplicatesDelta(BaseModel):
    delta: int
    delta_size: int
    remaining: int

    def __bool__(self) -> bool:
        return bool(self.delta or self.delta_size)  # NOT remaining


class CommitDelta(BaseModel):
    first: CommitInfo
    second: CommitInfo
    first_metadata: MetadataSummary
    second_metadata: MetadataSummary
    metadata_diff: MetadataDiff
    unique_assets: UniqueAssetsDelta
    duplicate_assets: DuplicatesDelta
    subjects: AddedRemoved

    def __bool__(self) -> bool:
        return any(
            bool(getattr(self, field))
            for field in [
                "metadata_diff",
                "unique_assets",
                "duplicate_assets",
                "subjects",
            ]
        )

    def to_markdown(self) -> str:
        s = f"## {self.first.short_id} versus {self.second.short_id}\n\n"
        if self.unique_assets or self.duplicate_assets or self.subjects:
            s += (
                f"* Assets added: {self.unique_assets.by_qty.added}"
                f" ({naturalsize(self.unique_assets.by_bytes.added)})\n"
            )
            s += (
                f"* Assets removed: {self.unique_assets.by_qty.removed}"
                f" ({naturalsize(self.unique_assets.by_bytes.removed)})\n"
            )
            s += (
                f"* Duplicates delta: {self.duplicate_assets.delta}"
                f" ({naturalsize(self.duplicate_assets.delta_size)})\n"
            )
            s += f"* Duplicates remaining: {self.duplicate_assets.remaining}\n"
            s += f"* Subjects added: {self.subjects.added}\n\n"
        else:
            s += "No change in assets\n\n"
        s += f"### Metadata summary for {self.first.short_id}\n\n"
        s += self.first_metadata.to_markdown() + "\n"
        s += f"### Metadata summary for {self.second.short_id}\n\n"
        s += self.second_metadata.to_markdown() + "\n"
        s += "### Changes in metadata\n\n"
        s += self.metadata_diff.to_markdown()
        return s


class Report(BaseModel):
    dandiset_id: str
    from_dt: datetime | None
    to_dt: datetime | None
    commit_delta: CommitDelta | None
    published_versions: list[CommitInfo]
    since_latest: CommitDelta | None

    def to_markdown(self) -> str:
        start_time = self.from_dt
        end_time = self.to_dt
        if self.commit_delta is not None:
            if start_time is None:
                start_time = self.commit_delta.first.created
            if end_time is None:
                end_time = self.commit_delta.second.created
            if self.since_latest is not None:
                assert start_time is not None
                assert end_time is not None
                start_time = min(start_time, self.since_latest.first.created)
                end_time = max(end_time, self.since_latest.first.created)
        s = (
            f"# Changes in [DANDI:{self.dandiset_id}]"
            f"(https://dandiarchive.org/dandiset/{self.dandiset_id}/)"
        )
        if start_time is not None:
            s += f" from {short_datetime(start_time)}"
        if end_time is not None:
            s += f" to {short_datetime(end_time)}"
        s += "\n\n"
        s += f"* **GitHub URL:** <https://github.com/dandisets/{self.dandiset_id}>\n"
        s += "* **Versions published:**"
        if self.published_versions:
            s += "\n"
            for v in self.published_versions:
                s += (
                    f"    * [{v.short_id}](https://dandiarchive.org/dandiset/"
                    f"{self.dandiset_id}/{v.short_id}/) [[GitHub URL]"
                    f"(https://github.com/dandisets/{self.dandiset_id}/"
                    f"releases/tag/{v.short_id})]\n"
                )
        else:
            s += " [none]\n"
        s += "\n"
        if self.since_latest is not None and not self.since_latest:
            s += (
                f"No changes since version {self.since_latest.first.short_id}"
                f" published on {short_datetime(self.since_latest.first.created)}\n\n"
            )
        if self.commit_delta is None:
            s += "**No changes in the given timeframe**\n"
        else:
            s += self.commit_delta.to_markdown()
        if self.since_latest is not None and self.since_latest:
            s += "\n\n" + self.since_latest.to_markdown()
        return s


class DandiDataSet(BaseModel):
    path: Path

    def readgit(self, *args: str | Path, **kwargs: Any) -> str:
        txt = subprocess.run(
            ["git", *args],
            cwd=self.path,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            **kwargs,
        ).stdout
        assert isinstance(txt, str)
        return txt.strip()

    def get_first_and_last_commit(
        self, from_dt: datetime | None, to_dt: datetime | None
    ) -> tuple[CommitInfo, CommitInfo]:
        # --before orders by commit date, not author date, so we need to filter
        # commits ourselves.
        cmtlines = self.readgit(
            "log",
            r"--grep=\[backups2datalad\]",
            r"--grep=Ran backups2datalad\.py",
            "--format=%H %aI %p",
        ).splitlines()
        commits: list[CommitInfo] = []
        warned_nonlinear = False
        for cmt in cmtlines:
            committish, created, *parents = cmt.strip().split()
            if len(parents) > 1 and not warned_nonlinear:
                log.warning("Commits in given timeframe are nonlinear")
                warned_nonlinear = True
            short_id = self.readgit("describe", "--always", committish)
            commits.append(
                CommitInfo(committish=committish, short_id=short_id, created=created)
            )
        commits.sort(key=attrgetter("created"))
        commits = filter_commits(commits, from_dt, to_dt)
        if len(commits) < 2:
            raise InsufficientCommitsError()
        return (commits[0], commits[-1])

    def get_tags(
        self, from_dt: datetime | None, to_dt: datetime | None
    ) -> list[CommitInfo]:
        # Tags are returned in ascending order of creation
        taglines = self.readgit(
            "tag",
            "-l",
            "--sort=creatordate",
            "--format=%(creatordate:iso-strict) %(refname:strip=2)",
        ).splitlines()
        tags: list[CommitInfo] = []
        for tl in taglines:
            ts, _, tag = tl.partition(" ")
            tags.append(CommitInfo(committish=tag, short_id=tag, created=ts))
        return filter_commits(tags, from_dt, to_dt)

    def get_assets(self, commit: CommitInfo) -> Iterator[AssetInfo]:
        repo = AnnexRepo(str(self.path))
        for p, info in repo.get_content_annexinfo(ref=commit.committish).items():
            relpath = p.relative_to(self.path)
            if info.get("type") == "file" and relpath.parts[0] not in IGNORED:
                path = str(PurePosixPath(relpath))
                subject: str | None
                if m := re.match(r"sub-([^/]+)/", path):
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

    def get_dandiset_metadata(self, commit: str | CommitInfo) -> dict:
        if isinstance(commit, str):
            committish = commit
        else:
            committish = commit.committish
        md = YAML(typ="safe").load(self.readgit("show", f"{committish}:dandiset.yaml"))
        assert isinstance(md, dict)
        return md

    def get_asset_metadata(self, commit: CommitInfo) -> list[dict] | None:
        assets = json.loads(
            self.readgit("show", f"{commit.committish}:.dandi/assets.json")
        )
        if assets and not (isinstance(assets[0], dict) and "asset_id" in assets[0]):
            return None
        else:
            return [a["metadata"] for a in assets]

    def cmp_commit_assets(
        self, commit1: CommitInfo, commit2: CommitInfo
    ) -> CommitDelta:
        asset_sizes: dict[str, int] = {}
        key_qtys: list[dict[str, int]] = []
        subjects_sets: list[set[str]] = []
        metadata_summaries: list[MetadataSummary] = []
        for cmt in [commit1, commit2]:
            keys: dict[str, int] = Counter()
            subjects: set[str] = set()
            for asset in self.get_assets(cmt):
                asset_sizes[asset.key] = asset.size
                keys[asset.key] += 1
                if asset.subject is not None:
                    subjects.add(asset.subject)
            key_qtys.append(keys)
            subjects_sets.append(subjects)
            metadata_summaries.append(
                MetadataSummary.from_metadata(
                    self.get_dandiset_metadata(cmt),
                    self.get_asset_metadata(cmt),
                )
            )
        keys1, keys2 = key_qtys
        added_keys = keys2.keys() - keys1.keys()
        removed_keys = keys1.keys() - keys2.keys()
        duplicates1 = Counter({k: n - 1 for k, n in keys1.items() if n > 1})
        duplicates2 = Counter({k: n - 1 for k, n in keys2.items() if n > 1})
        subjects1, subjects2 = subjects_sets
        mds1, mds2 = metadata_summaries
        return CommitDelta(
            first=commit1,
            second=commit2,
            first_metadata=mds1,
            second_metadata=mds2,
            metadata_diff=MetadataDiff.compare(mds1, mds2),
            unique_assets={
                "by_qty": {
                    "added": len(added_keys),
                    "removed": len(removed_keys),
                },
                "by_bytes": {
                    "added": sum(asset_sizes[k] for k in added_keys),
                    "removed": sum(asset_sizes[k] for k in removed_keys),
                },
            },
            duplicate_assets={
                "delta": sum(duplicates2.values()) - sum(duplicates1.values()),
                "delta_size": sum(asset_sizes[k] * n for k, n in duplicates2.items())
                - sum(asset_sizes[k] * n for k, n in duplicates1.items()),
                "remaining": sum(duplicates2.values()),
            },
            subjects={
                "added": len(subjects2 - subjects1),
                "removed": len(subjects1 - subjects2),
            },
        )


class InsufficientCommitsError(Exception):
    pass


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
    from_dt: datetime | None,
    to_dt: datetime | None,
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
    dd = DandiDataSet(path=dandiset)
    did = re.sub(r"^.+?:", "", dd.get_dandiset_metadata("HEAD")["identifier"])
    tags = dd.get_tags(from_dt, to_dt)
    commit_delta: CommitDelta | None
    since_latest: CommitDelta | None
    try:
        commit1, commit2 = dd.get_first_and_last_commit(from_dt, to_dt)
    except InsufficientCommitsError:
        commit_delta = None
        since_latest = None
    else:
        commit_delta = dd.cmp_commit_assets(commit1, commit2)
        if tags:
            since_latest = dd.cmp_commit_assets(tags[-1], commit2)
        else:
            since_latest = None
    report = Report(
        dandiset_id=did,
        from_dt=from_dt,
        to_dt=to_dt,
        commit_delta=commit_delta,
        published_versions=tags,
        since_latest=since_latest,
    )
    if fmt == "json":
        print(report.model_dump_json(indent=4), file=outfile)
    elif fmt == "yaml":
        yaml = YAML(typ="safe")
        yaml.default_flow_style = False
        yaml.dump(report.dict(), outfile)
    elif fmt == "markdown":
        print(report.to_markdown(), file=outfile, end="")


def filter_commits(
    commits: list[CommitInfo], from_dt: datetime | None, to_dt: datetime | None
) -> list[CommitInfo]:
    # `commits` must be sorted by `created` in ascending order
    if from_dt is not None:
        while commits and commits[0].created < from_dt:
            commits.pop(0)
    if to_dt is not None:
        while commits and commits[-1].created >= to_dt:
            commits.pop(-1)
    return commits


def short_datetime(dt: datetime) -> str:
    if dt.timetz() == time(0, tzinfo=timezone.utc):
        return dt.strftime("%Y-%m-%d")
    elif dt.time() == time(0):
        return dt.strftime("%Y-%m-%d %z")
    else:
        return str(dt)


if __name__ == "__main__":
    main()
