from __future__ import annotations

from dataclasses import InitVar, dataclass, field, replace
from datetime import datetime
from enum import Enum
from functools import partial
import json
from operator import attrgetter
from pathlib import Path
import subprocess
import sys
from typing import Any, Optional, Sequence, cast

import anyio
from datalad.api import Dataset
from datalad.runner.exception import CommandError

from .aioutil import areadcmd, aruncmd, open_git_annex, stream_null_command
from .config import Remote
from .consts import DEFAULT_BRANCH
from .logging import log
from .util import custom_commit_env

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class AsyncDataset:
    ds: Dataset = field(init=False)
    dirpath: InitVar[str | Path]

    def __post_init__(self, dirpath: str | Path) -> None:
        self.ds = Dataset(dirpath)

    @property
    def path(self) -> str:
        return cast(str, self.ds.path)

    @property
    def pathobj(self) -> Path:
        return cast(Path, self.ds.pathobj)

    async def ensure_installed(
        self,
        desc: str,
        commit_date: Optional[datetime] = None,
        backup_remote: Optional[Remote] = None,
        backend: str = "SHA256E",
        cfg_proc: Optional[str] = "text2git",
    ) -> bool:
        # Returns True if the dataset was freshly created
        if self.ds.is_installed():
            return False
        log.info("Creating dataset for %s", desc)
        argv = ["datalad", "-c", f"datalad.repo.backend={backend}", "create"]
        if cfg_proc is not None:
            argv.append("-c")
            argv.append(cfg_proc)
        argv.append(self.path)
        await aruncmd(
            *argv,
            env={
                **custom_commit_env(commit_date),
                "GIT_CONFIG_PARAMETERS": f"'init.defaultBranch={DEFAULT_BRANCH}'",
            },
        )
        if backup_remote is not None:
            await self.call_annex(
                "initremote",
                backup_remote.name,
                f"type={backup_remote.type}",
                *[f"{k}={v}" for k, v in backup_remote.options.items()],
            )
            await self.call_annex("untrust", backup_remote.name)
            await self.call_annex(
                "wanted",
                backup_remote.name,
                "(not metadata=distribution-restrictions=*)",
            )
        log.debug("Dataset for %s created", desc)
        return True

    async def is_dirty(self) -> bool:
        return await anyio.to_thread.run_sync(attrgetter("dirty"), self.ds.repo)

    async def is_unclean(self) -> bool:
        def _unclean() -> bool:
            return any(
                r["state"] != "clean" for r in self.ds.status(result_renderer=None)
            )

        return await anyio.to_thread.run_sync(_unclean)

    async def get_repo_config(self, key: str) -> Optional[str]:
        try:
            return await areadcmd("git", "config", "--get", key, cwd=self.path)
        except subprocess.CalledProcessError:
            return None

    async def call_git(self, *args: str | Path, **kwargs: Any) -> None:
        await aruncmd("git", *args, cwd=self.path, **kwargs)

    async def read_git(self, *args: str | Path, **kwargs: Any) -> str:
        return await areadcmd("git", *args, cwd=self.path, **kwargs)

    async def call_annex(self, *args: str | Path, **kwargs: Any) -> None:
        await aruncmd("git-annex", *args, cwd=self.path, **kwargs)

    async def save(
        self,
        message: str,
        path: Sequence[str | Path] = (),
        commit_date: Optional[datetime] = None,
    ) -> None:
        # TODO: Improve
        await aruncmd(
            "datalad",
            "save",
            "-m",
            message,
            *path,
            cwd=self.path,
            env=custom_commit_env(commit_date),
        )

    async def push(self, to: str, jobs: int, data: Optional[str] = None) -> None:
        i = 0
        while True:
            try:
                # TODO: Improve
                await anyio.to_thread.run_sync(
                    partial(self.ds.push, to=to, jobs=jobs, data=data)
                )
            except CommandError as e:
                if "unexpected disconnect" in str(e):
                    i += 1
                    if i < 3:
                        log.warning(
                            "Push of dataset at %s failed with unexpected"
                            " disconnect; retrying",
                            self.path,
                        )
                        continue
                raise
            else:
                break

    async def gc(self) -> None:
        try:
            await self.call_git("gc")
        except subprocess.CalledProcessError as e:
            if e.returncode == 128:
                log.warning("`git gc` in %s exited with code 128", self.path)
            else:
                raise

    async def add(self, path: str) -> None:
        # `path` must be relative to the root of the dataset
        await self.call_annex("add", path)

    async def remove(self, path: str) -> None:
        # `path` must be relative to the root of the dataset
        await self.call_git("rm", path)

    async def update(self, how: str, sibling: Optional[str] = None) -> None:
        await anyio.to_thread.run_sync(
            partial(self.ds.update, how=how, sibling=sibling)
        )

    async def get_file_stats(self) -> list[FileStat]:
        filedict: dict[str, FileStat] = {}
        async with aclosing(
            stream_null_command(
                "git",
                "ls-tree",
                "-lrz",
                "HEAD",
                cwd=self.pathobj,
            )
        ) as p:
            async for entry in p:
                fst = FileStat.from_entry(entry)
                filedict[fst.path] = fst
        async with await open_git_annex(
            "find", "--include=*", "--json", use_stdin=False, path=self.pathobj
        ) as p:
            async for line in p:
                data = json.loads(line)
                path = cast(str, data["file"])
                filedict[path] = replace(filedict[path], size=int(data["bytesize"]))
        return list(filedict.values())

    async def create_github_sibling(
        self,
        owner: str,
        name: str,
        backup_remote: Optional[Remote],
        *,
        existing: str = "skip",
    ) -> bool:
        # Returns True iff sibling was created
        if "github" not in (await self.read_git("remote")).splitlines():
            log.info("Creating GitHub sibling for %s", name)
            await anyio.to_thread.run_sync(
                partial(
                    self.ds.create_sibling_github,
                    reponame=name,
                    existing=existing,
                    name="github",
                    access_protocol="https",
                    github_organization=owner,
                    publish_depends=backup_remote.name
                    if backup_remote is not None
                    else None,
                )
            )
            for key, value in [
                ("remote.github.pushurl", f"git@github.com:{owner}/{name}.git"),
                (f"branch.{DEFAULT_BRANCH}.remote", "github"),
                (f"branch.{DEFAULT_BRANCH}.merge", f"refs/heads/{DEFAULT_BRANCH}"),
            ]:
                await self.call_git("config", "--local", "--replace-all", key, value)
            return True
        else:
            log.debug("GitHub remote already exists for %s", name)
            return False


class ObjectType(Enum):
    COMMIT = "commit"
    BLOB = "blob"
    TREE = "tree"


@dataclass
class FileStat:
    path: str
    type: ObjectType
    size: Optional[int]

    @classmethod
    def from_entry(cls, entry: str) -> FileStat:
        stats, _, path = entry.partition("\t")
        _, typename, _, sizestr = stats.split()
        return cls(
            path=path,
            type=ObjectType(typename),
            size=None if sizestr == "-" else int(sizestr),
        )