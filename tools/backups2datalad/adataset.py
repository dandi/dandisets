from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from datetime import datetime
from enum import Enum
from functools import partial
from operator import attrgetter
from pathlib import Path
import subprocess
from typing import Any, AsyncIterator, Optional, Sequence, cast

import anyio
from anyio.streams.text import TextReceiveStream
from datalad.api import Dataset

from .aioutil import areadcmd, aruncmd
from .config import Remote
from .consts import DEFAULT_BRANCH
from .logging import log
from .util import custom_commit_env


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
        # TODO: Improve
        await anyio.to_thread.run_sync(
            partial(self.ds.push, to=to, jobs=jobs, data=data)
        )

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
        await self.call_git("add", path)

    async def remove(self, path: str) -> None:
        # `path` must be relative to the root of the dataset
        await self.call_git("rm", path)

    async def update(self, how: str, sibling: Optional[str] = None) -> None:
        await anyio.to_thread.run_sync(
            partial(self.ds.update, how=how, sibling=sibling)
        )

    async def aiter_file_stats(self) -> AsyncIterator[FileStat]:
        async with await anyio.open_process(
            [
                "git",
                "ls-tree",
                "-r",
                "--format=%(objecttype):%(objectsize):%(path)",
                "HEAD",
            ],
            cwd=self.path,
            stderr=None,
        ) as p:
            buff = ""
            assert p.stdout is not None
            async for text in TextReceiveStream(p.stdout):
                while True:
                    try:
                        i = text.index("\0")
                    except ValueError:
                        buff = text
                        break
                    else:
                        yield FileStat.from_entry(buff + text[:i])
                        buff = ""
                        text = text[i + 1 :]
            if buff:
                yield FileStat.from_entry(buff)
            ### TODO: Raise an exception if p.returncode is nonzero?

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
        typename, sizestr, path = entry.split(":", maxsplit=2)
        return cls(
            path=path,
            type=ObjectType(typename),
            size=None if sizestr == "-" else int(sizestr),
        )
