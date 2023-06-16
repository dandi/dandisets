from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
from typing import Any, List, cast

from dandi.utils import find_files

from backups2datalad.util import is_meta_file


@dataclass
class GitRepo:
    path: Path

    def runcmd(self, *args: str | Path, **kwargs: Any) -> subprocess.CompletedProcess:
        return subprocess.run(["git", *args], cwd=self.path, **kwargs)

    def readcmd(self, *args: str | Path) -> str:
        r = self.runcmd(*args, stdout=subprocess.PIPE, text=True, check=True)
        assert isinstance(r.stdout, str)
        return r.stdout.strip()

    def get_tag_date(self, tag: str) -> str:
        return self.readcmd(
            "for-each-ref", "--format=%(creatordate:iso-strict)", f"refs/tags/{tag}"
        )

    def get_tag_creator(self, tag: str) -> str:
        # `tag` must be an annotated tag.
        return self.readcmd(
            "for-each-ref", "--format=%(taggername) %(taggeremail)", f"refs/tags/{tag}"
        )

    def get_commit_date(self, commitish: str) -> str:
        return self.readcmd("show", "-s", "--format=%aI", f"{commitish}^{{commit}}")

    def get_commit_author(self, commitish: str) -> str:
        return self.readcmd(
            "show", "-s", "--format=%an <%ae>", f"{commitish}^{{commit}}"
        )

    def get_commit_subject(self, commitish: str) -> str:
        return self.readcmd("show", "-s", "--format=%s", f"{commitish}^{{commit}}")

    def get_commit_message(self, commitish: str) -> str:
        return self.readcmd("show", "-s", "--format=%B", f"{commitish}^{{commit}}")

    def get_commitish_hash(self, commitish: str) -> str:
        return self.readcmd("rev-parse", f"{commitish}^{{commit}}")

    def is_ancestor(self, commit1: str, commit2: str) -> bool:
        return (
            self.runcmd("merge-base", "--is-ancestor", commit1, commit2).returncode == 0
        )

    def parent_is_ancestor(self, commit1: str, commit2: str) -> bool:
        return (
            self.runcmd(
                "merge-base", "--is-ancestor", f"{commit1}^", commit2
            ).returncode
            == 0
        )

    def get_blob(self, treeish: str, path: str) -> str:
        return self.readcmd("show", f"{treeish}:{path}")

    def get_tags(self) -> list[str]:
        return self.readcmd("tag", "-l", "--sort=creatordate").splitlines()

    def get_diff_tree(self, commitish: str) -> dict[str, str]:
        stat = self.readcmd(
            "diff-tree", "--no-commit-id", "--name-status", "-r", commitish
        )
        status: dict[str, str] = {}
        for line in stat.splitlines():
            sym, _, path = line.partition("\t")
            status[path] = sym
        return status

    def get_backup_commits(self) -> list[str]:
        return self.readcmd(
            "rev-list", "--tags", r"--grep=\[backups2datalad\]", "HEAD"
        ).splitlines()

    def get_asset_files(self, commitish: str) -> set[str]:
        return {
            fname
            for fname in self.readcmd(
                "ls-tree", "-r", "--name-only", commitish
            ).splitlines()
            if not is_meta_file(fname, dandiset=True)
        }

    def get_assets_json(self, commitish: str) -> list[dict]:
        # We need to use typing.List here for Python 3.8 compatibility
        return cast(
            List[dict], json.loads(self.get_blob(commitish, ".dandi/assets.json"))
        )

    def get_commit_count(self) -> int:
        return int(self.readcmd("rev-list", "--count", "HEAD"))


def find_filepaths(dirpath: Path) -> Iterator[Path]:
    return map(
        Path,
        find_files(
            r".*",
            [dirpath],
            exclude_dotfiles=False,
            exclude_dotdirs=False,
            exclude_vcs=False,
        ),
    )
