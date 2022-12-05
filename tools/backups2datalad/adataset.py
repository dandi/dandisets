from __future__ import annotations

from collections import Counter
from dataclasses import InitVar, dataclass, field, replace
from datetime import datetime
from enum import Enum
from functools import partial
from operator import attrgetter
from pathlib import Path
import re
import subprocess
import sys
import textwrap
from typing import Any, AsyncGenerator, ClassVar, Optional, Sequence, cast

import anyio
from dandi.support.digests import ZCTree
from datalad.api import Dataset
from datalad.runner.exception import CommandError
from ghrepo import GHRepo
from pydantic import BaseModel

from .aioutil import areadcmd, aruncmd, stream_lines_command, stream_null_command
from .config import BackupConfig, Remote
from .consts import DEFAULT_BRANCH, GIT_OPTIONS
from .logging import log
from .util import custom_commit_env, exp_wait, is_meta_file, key2hash

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class AsyncDataset:
    ds: Dataset = field(init=False)
    dirpath: InitVar[str | Path]
    lock: anyio.Lock = field(init=False, default_factory=anyio.Lock)

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
            return await self.read_git("config", "--get", key)
        except subprocess.CalledProcessError as e:
            if e.returncode == 1:
                return None
            else:
                raise

    async def get_datalad_id(self) -> str:
        return await self.read_git(
            "config", "--file", ".datalad/config", "--get", "datalad.dataset.id"
        )

    async def call_git(self, *args: str | Path, **kwargs: Any) -> None:
        await aruncmd(
            "git",
            *GIT_OPTIONS,
            *args,
            cwd=self.path,
            **kwargs,
        )

    async def read_git(self, *args: str | Path, **kwargs: Any) -> str:
        return await areadcmd(
            "git",
            *GIT_OPTIONS,
            *args,
            cwd=self.path,
            **kwargs,
        )

    async def call_annex(self, *args: str | Path, **kwargs: Any) -> None:
        await aruncmd("git", *GIT_OPTIONS, "annex", *args, cwd=self.path, **kwargs)

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
            "-d",
            ".",
            "-m",
            message,
            *path,
            cwd=self.path,
            env=custom_commit_env(commit_date),
        )

    async def commit(
        self,
        message: str,
        commit_date: Optional[datetime] = None,
        paths: Sequence[str | Path] = (),
        check_dirty: bool = True,
    ) -> None:
        """Use git commit directly, verify that all is committed

        Raises RuntimeError if dataset remains dirty.

        Primarily to be used to overcome inability of datalad save to save
        updated states of subdatasets without them being installed in the tree.
        Ref: https://github.com/datalad/datalad/issues/7074
        """
        await self.call_git(
            "commit",
            "-m",
            message,
            "--",
            *map(str, paths),
            env=custom_commit_env(commit_date),
        )
        if check_dirty and await self.is_dirty():
            raise RuntimeError(
                f"{self.path} is still dirty after committing."
                "  Please check if all changes were staged."
            )

    async def push(self, to: str, jobs: int, data: Optional[str] = None) -> None:
        waits = exp_wait(attempts=6, base=2.1)
        while True:
            try:
                # TODO: Improve
                await anyio.to_thread.run_sync(
                    partial(self.ds.push, to=to, jobs=jobs, data=data)
                )
            except CommandError as e:
                if "unexpected disconnect" in str(e):
                    try:
                        delay = next(waits)
                    except StopIteration:
                        raise e
                    log.warning(
                        "Push of dataset at %s failed with unexpected"
                        " disconnect; retrying",
                        self.path,
                    )
                    await anyio.sleep(delay)
                    continue
                else:
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
        # to avoid problems with locking etc. Same is done in DataLad's invocation
        # of rm
        self.ds.repo.precommit()
        # `path` must be relative to the root of the dataset
        try:
            async with self.lock:
                await self.call_git(
                    "rm",
                    "-f",
                    "--ignore-unmatch",
                    "--",
                    path,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
        except subprocess.CalledProcessError as e:
            lockfile = self.pathobj / ".git" / "index.lock"
            output = e.stdout.decode("utf-8")
            if lockfile.exists() and str(lockfile) in output:
                r = await aruncmd("fuser", "-v", lockfile, stdout=subprocess.PIPE)
                log.error(
                    "%s: Unable to remove %s due to lockfile; `fuser -v` output"
                    " on lockfile:\n%s",
                    self.pathobj,
                    path,
                    textwrap.indent(r.stdout.decode("utf-8"), "> "),
                )
            else:
                log.error(
                    "%s: `git rm` on %s failed with output:\n%s",
                    self.pathobj,
                    path,
                    textwrap.indent(output, "> "),
                )
            raise

    async def update(self, how: str, sibling: Optional[str] = None) -> None:
        await anyio.to_thread.run_sync(
            partial(self.ds.update, how=how, sibling=sibling)
        )

    async def get_file_stats(self) -> list[FileStat]:
        filedict: dict[str, FileStat] = {}
        async with aclosing(
            stream_null_command("git", "ls-tree", "-lrz", "HEAD", cwd=self.pathobj)
        ) as p:
            async for entry in p:
                try:
                    fst = FileStat.from_entry(entry)
                except Exception:
                    log.exception(
                        "Error parsing ls-tree line %r for %s:", entry, self.path
                    )
                    raise
                filedict[fst.path] = fst
        async with aclosing(self.aiter_annexed_files()) as afiles:
            async for f in afiles:
                filedict[f.file] = replace(filedict[f.file], size=f.bytesize)
        return list(filedict.values())

    async def aiter_annexed_files(self) -> AsyncGenerator[AnnexedFile, None]:
        async with aclosing(
            stream_lines_command(
                "git",
                *GIT_OPTIONS,
                "annex",
                "find",
                "--include=*",
                "--json",
                cwd=self.pathobj,
            )
        ) as p:
            async for line in p:
                try:
                    data = AnnexedFile.parse_raw(line)
                except Exception:
                    log.exception(
                        "Error parsing `git-annex find` output for %s: bad"
                        " output line %r",
                        self.path,
                        line,
                    )
                    raise
                else:
                    yield data

    async def compute_zarr_checksum(self) -> str:
        log.debug(
            "Computing Zarr checksum for locally-annexed files in %s", self.pathobj
        )
        zcc = ZCTree()
        # rely on the fact that every data component of zarr folder is in annex
        # and we keep .dandi/ folder content directly in git
        async with aclosing(self.aiter_annexed_files()) as afiles:
            async for f in afiles:
                if f.backend not in ("MD5", "MD5E"):
                    raise RuntimeError(
                        f"{f.file} in {self.pathobj} has {f.backend} backend"
                        " instead of MD5 or MD5E required for Zarr checksum"
                    )
                zcc.add(Path(f.file), key2hash(f.key), f.bytesize)
        checksum = cast(str, zcc.get_digest())
        log.debug("Computed Zarr checksum %s for %s", checksum, self.pathobj)
        return checksum

    async def create_github_sibling(
        self,
        owner: str,
        name: str,
        backup_remote: Optional[Remote],
        *,
        existing: str = "reconfigure",
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

    async def get_remote_url(self) -> str:
        upstream = await self.get_repo_config(f"branch.{DEFAULT_BRANCH}.remote")
        if upstream is None:
            raise ValueError(
                f"Upstream branch not set for {DEFAULT_BRANCH} in {self.path}"
            )
        url = await self.get_repo_config(f"remote.{upstream}.url")
        if url is None:
            raise ValueError(f"{upstream!r} remote URL not set for {self.path}")
        return url

    async def get_ghrepo(self) -> GHRepo:
        url = await self.get_remote_url()
        return GHRepo.parse_url(url)

    async def get_stats(
        self,
        config: BackupConfig,  # for path to zarrs
    ) -> DatasetStats:
        stored_stats = await self.get_stored_stats()
        if stored_stats is not None:
            # stats were stored and state of the dataset did not change since then
            return stored_stats
        log.info("%s: Counting up files ...", self.path)
        files = 0
        size = 0
        # get them all and remap per path
        subdatasets = {s["path"]: s for s in await self.get_subdatasets()}
        for filestat in await self.get_file_stats():
            path = Path(filestat.path)
            if not is_meta_file(path.parts[0], dandiset=True):
                if filestat.type is ObjectType.COMMIT:
                    # this zarr should not be present locally as a submodule
                    # so we should get its id from its information in submodules.
                    sub_info = subdatasets[str(self.pathobj / path)]
                    _, zarr_stat = await self.get_zarr_sub_stats(sub_info, config)
                    files += zarr_stat.files
                    size += zarr_stat.size
                else:
                    files += 1
                    assert filestat.size is not None
                    size += filestat.size
        log.info("%s: Done counting up files", self.path)
        stats = DatasetStats(files=files, size=size)
        await self.store_stats(stats)
        return stats

    @staticmethod
    async def get_zarr_sub_stats(
        sub_info: dict, config: BackupConfig
    ) -> tuple[str, DatasetStats]:
        zarr_id = Path(sub_info["gitmodule_url"]).name
        assert config.zarr_root is not None
        zarr_ds = AsyncDataset(config.zarr_root / zarr_id)
        # here we assume that HEAD among dandisets is the same as of
        # submodule, which might not necessarily be the case.
        # TODO: get for the specific commit
        zarr_stat = await zarr_ds.get_stats(config=config)
        return zarr_id, zarr_stat

    async def get_stored_stats(self) -> Optional[DatasetStats]:
        if (stored_stats := self.ds.config.get("dandi.stats", None)) is not None:
            try:
                stored_commit, files_str, size_str = stored_stats.split(",")
                files = int(files_str)
                size = int(size_str)
            except Exception:
                return None
            if stored_commit == await self.get_commit_hash():
                return DatasetStats(files=files, size=size)
        return None

    async def store_stats(self, stats: DatasetStats) -> None:
        commit = await self.get_commit_hash()
        value = f"{commit},{stats.files},{stats.size}"
        self.ds.config.set("dandi.stats", value, scope="local")

    async def get_commit_hash(self) -> str:
        return await self.read_git("show", "-s", "--format=%H")

    def assert_no_duplicates_in_gitmodules(self) -> None:
        filepath = self.pathobj / ".gitmodules"
        if not filepath.exists():
            return
        qtys: Counter[str] = Counter()
        with filepath.open() as fp:
            for line in fp:
                if m := re.fullmatch(r'\[submodule "(.+)"\]\s*', line):
                    qtys[m[1]] += 1
        dupped = [name for (name, count) in qtys.most_common() if count > 1]
        assert not dupped, f"Duplicates found in {filepath}: {dupped}"

    def get_assets_state(self) -> Optional[AssetsState]:
        try:
            return AssetsState.parse_file(self.pathobj / AssetsState.PATH)
        except FileNotFoundError:
            return None

    async def set_assets_state(self, state: AssetsState) -> None:
        path = self.pathobj / AssetsState.PATH
        path.parent.mkdir(exist_ok=True)
        path.write_text(state.json(indent=4) + "\n")
        await self.add(str(AssetsState.PATH))

    async def get_subdatasets(self, **kwargs: Any) -> list:
        return await anyio.to_thread.run_sync(
            partial(self.ds.subdatasets, result_renderer=None, **kwargs)
        )

    async def uninstall_subdatasets(self) -> None:
        # dropping all dandisets is not trivial :-/
        # https://github.com/datalad/datalad/issues/7013
        #  --reckless kill is not working
        # https://github.com/datalad/datalad/issues/6933#issuecomment-1239402621
        #   '*' pathspec is not supported
        # so could resort to this ad-hoc way but we might want just to pair
        subdatasets = await self.get_subdatasets(result_xfm="relpaths", state="present")
        if subdatasets:
            log.debug("Will uninstall %d subdatasets", len(subdatasets))
            res = await anyio.to_thread.run_sync(
                partial(
                    self.ds.drop,
                    what="datasets",
                    recursive=True,
                    path=subdatasets,
                    reckless="kill",
                )
            )
            assert all(r["status"] == "ok" for r in res)
        else:
            # yet another case where [] is treated as None?
            log.debug("No subdatasets to uninstall")

    async def add_submodule(self, path: str, url: str, datalad_id: str) -> None:
        await self.call_git("submodule", "add", "--", url, path)
        await self.call_git(
            "config",
            "--file",
            ".gitmodules",
            "--replace-all",
            f"submodule.{path}.datalad-id",
            datalad_id,
        )
        await self.add(".gitmodules")

    async def update_submodule(self, path: str, commit_hash: str) -> None:
        await self.call_git(
            "update-index",
            "-z",
            # apparently must be the last argument!
            "--index-info",
            input=f"160000 commit {commit_hash}\t{path}\0".encode("utf-8"),
        )


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


@dataclass
class DatasetStats:
    files: int
    size: int


class AssetsState(BaseModel):
    PATH: ClassVar[Path] = Path(".dandi", "assets-state.json")
    timestamp: datetime


class AnnexedFile(BaseModel):
    backend: str
    bytesize: int
    # error-messages: list[str]
    file: str
    # hashdirlower: str
    # hashdirmixed: str
    # humansize: str
    key: str
    # keyname: str
    # mtime: Literal["unknown"] | ???
