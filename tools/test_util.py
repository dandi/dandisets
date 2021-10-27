from dataclasses import dataclass
from pathlib import Path
import subprocess
from typing import Any, Dict, List, Union


@dataclass
class GitRepo:
    path: Path

    def runcmd(
        self, *args: Union[str, Path], **kwargs: Any
    ) -> subprocess.CompletedProcess:
        return subprocess.run(["git", *args], cwd=self.path, **kwargs)

    def readcmd(self, *args: Union[str, Path], **kwargs: Any) -> str:
        kwargs["check"] = True
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

    def get_tags(self) -> List[str]:
        return self.readcmd("tag", "-l", "--sort=creatordate").splitlines()

    def get_diff_tree(self, commitish: str) -> Dict[str, str]:
        stat = self.readcmd(
            "diff-tree", "--no-commit-id", "--name-status", "-r", commitish
        )
        status: Dict[str, str] = {}
        for line in stat.splitlines():
            sym, _, path = line.partition("\t")
            status[path] = sym
        return status
