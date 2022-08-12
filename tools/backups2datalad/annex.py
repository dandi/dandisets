from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import json
from pathlib import Path
import sys
from typing import AsyncGenerator, Optional

import anyio

from .aioutil import TextProcess, open_git_annex, stream_null_command
from .logging import log
from .util import format_errors

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class AsyncAnnex(anyio.abc.AsyncResource):
    repo: Path
    digest_type: str = "SHA256"
    pfromkey: Optional[TextProcess] = None
    pexaminekey: Optional[TextProcess] = None
    pwhereis: Optional[TextProcess] = None
    pregisterurl: Optional[TextProcess] = None
    locks: dict[str, anyio.Lock] = field(
        init=False, default_factory=lambda: defaultdict(anyio.Lock)
    )

    async def aclose(self) -> None:
        for p in [self.pfromkey, self.pexaminekey, self.pwhereis, self.pregisterurl]:
            if p is not None:
                await p.aclose()

    async def from_key(self, key: str, path: str) -> None:
        async with self.locks["fromkey"]:
            if self.pfromkey is None:
                self.pfromkey = await open_git_annex(
                    "fromkey",
                    "--force",
                    "--batch",
                    "--json",
                    "--json-error-messages",
                    path=self.repo,
                )
            await self.pfromkey.send(f"{key} {path}\n")
            r = json.loads(await self.pfromkey.receive())
        if not r["success"]:
            log.error(
                "`git annex fromkey %s %s` [cwd=%s] call failed:%s",
                key,
                path,
                self.repo,
                format_errors(r["error-messages"]),
            )
            ### TODO: Raise an exception?

    async def mkkey(self, filename: str, size: int, digest: str) -> str:
        async with self.locks["examinekey"]:
            if self.pexaminekey is None:
                self.pexaminekey = await open_git_annex(
                    "examinekey",
                    "--batch",
                    f"--migrate-to-backend={self.digest_type}E",
                    path=self.repo,
                )
            await self.pexaminekey.send(
                f"{self.digest_type}-s{size}--{digest} {filename}\n"
            )
            return (await self.pexaminekey.receive()).strip()

    async def get_key_remotes(self, key: str) -> Optional[list[str]]:
        # Returns None if key is not known to git-annex
        async with self.locks["whereis"]:
            if self.pwhereis is None:
                self.pwhereis = await open_git_annex(
                    "whereis",
                    "--batch-keys",
                    "--json",
                    "--json-error-messages",
                    path=self.repo,
                    warn_on_fail=False,
                )
            await self.pwhereis.send(f"{key}\n")
            whereis = json.loads(await self.pwhereis.receive())
        if whereis["success"]:
            return [
                w["description"].strip("[]")
                for w in whereis["whereis"] + whereis["untrusted"]
            ]
        else:
            return None

    async def register_url(self, key: str, url: str) -> None:
        async with self.locks["registerurl"]:
            if self.pregisterurl is None:
                self.pregisterurl = await open_git_annex(
                    "registerurl",
                    "-c",
                    "annex.alwayscompact=false",
                    "--batch",
                    "--json",
                    "--json-error-messages",
                    path=self.repo,
                )
            await self.pregisterurl.send(f"{key} {url}\n")
            r = json.loads(await self.pregisterurl.receive())
        if not r["success"]:
            log.error(
                "`git annex registerurl %s %s` [cwd=%s] call failed:%s",
                key,
                url,
                self.repo,
                format_errors(r["error-messages"]),
            )
            ### TODO: Raise an exception?

    async def list_files(self) -> AsyncGenerator[str, None]:
        async with aclosing(
            stream_null_command(
                "git",
                "ls-tree",
                "-r",
                "--name-only",
                "-z",
                "HEAD",
                cwd=self.repo,
            )
        ) as p:
            async for fname in p:
                yield fname
