from dataclasses import dataclass
import json
from pathlib import Path
import textwrap
from typing import List, Optional

import trio

from . import log
from .util import TextProcess, open_git_annex


@dataclass
class AsyncAnnex(trio.abc.AsyncResource):
    repo: Path
    pfromkey: Optional[TextProcess] = None
    pexaminekey: Optional[TextProcess] = None
    pwhereis: Optional[TextProcess] = None
    pregisterurl: Optional[TextProcess] = None

    async def aclose(self) -> None:
        for p in [self.pfromkey, self.pexaminekey, self.pwhereis, self.pregisterurl]:
            if p is not None:
                await p.aclose()

    async def from_key(self, key: str, path: str) -> None:
        if self.pfromkey is None:
            self.pfromkey = await open_git_annex(
                "fromkey",
                "--force",
                "--batch",
                "--json",
                "--json-error-messages",
                path=self.repo,
            )
        async with self.pfromkey.lock:
            await self.pfromkey.send(f"{key} {path}\n")
            ### TODO: Do something if readline() returns "" (signalling EOF)
            r = json.loads(await self.pfromkey.readline())
        if not r["success"]:
            log.error(
                "`git annex fromkey %s %s` call failed!  Error messages:\n\n%s",
                key,
                path,
                textwrap.indent("".join(r["error-messages"]), " " * 4),
            )
            ### TODO: Raise an exception?

    async def mkkey(self, filename: str, size: int, sha256_digest: str) -> str:
        if self.pexaminekey is None:
            self.pexaminekey = await open_git_annex(
                "examinekey", "--batch", "--migrate-to-backend=SHA256E", path=self.repo
            )
        async with self.pexaminekey.lock:
            await self.pexaminekey.send(f"SHA256-s{size}--{sha256_digest} {filename}\n")
            ### TODO: Do something if readline() returns "" (signalling EOF)
            return (await self.pexaminekey.readline()).strip()

    async def get_key_remotes(self, key: str) -> Optional[List[str]]:
        # Returns None if key is not known to git-annex
        if self.pwhereis is None:
            self.pwhereis = await open_git_annex(
                "whereis",
                "--batch-keys",
                "--json",
                "--json-error-messages",
                path=self.repo,
            )
        async with self.pwhereis.lock:
            await self.pwhereis.send(f"{key}\n")
            ### TODO: Do something if readline() returns "" (signalling EOF)
            whereis = json.loads(await self.pwhereis.readline())
        if whereis["success"]:
            return [
                w["description"].strip("[]")
                for w in whereis["whereis"] + whereis["untrusted"]
            ]
        else:
            return None

    async def register_url(self, key: str, url: str) -> None:
        if self.pregisterurl is None:
            self.pregisterurl = await open_git_annex(
                "registerurl", "--batch", path=self.repo
            )
        await self.pregisterurl.send(f"{key} {url}\n")
