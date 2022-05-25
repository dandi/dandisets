from collections import defaultdict
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional

import anyio
from anyio.streams.text import TextReceiveStream

from .util import TextProcess, format_errors, log, open_git_annex


@dataclass
class AsyncAnnex(anyio.abc.AsyncResource):
    repo: Path
    nursery: anyio.abc.TaskGroup
    digest_type: str = "SHA256"
    pfromkey: Optional[TextProcess] = None
    pexaminekey: Optional[TextProcess] = None
    pwhereis: Optional[TextProcess] = None
    pregisterurl: Optional[TextProcess] = None
    locks: Dict[str, anyio.Lock] = field(
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
                    self.nursery,
                    "fromkey",
                    "--force",
                    "--batch",
                    "--json",
                    "--json-error-messages",
                    path=self.repo,
                )
            await self.pfromkey.send(f"{key} {path}\n")
            ### TODO: Do something if readline() returns "" (signalling EOF)
            r = json.loads(await self.pfromkey.readline())
        if not r["success"]:
            log.error(
                "`git annex fromkey %s %s` call failed:%s",
                key,
                path,
                format_errors(r["error-messages"]),
            )
            ### TODO: Raise an exception?

    async def mkkey(self, filename: str, size: int, digest: str) -> str:
        async with self.locks["examinekey"]:
            if self.pexaminekey is None:
                self.pexaminekey = await open_git_annex(
                    self.nursery,
                    "examinekey",
                    "--batch",
                    f"--migrate-to-backend={self.digest_type}E",
                    path=self.repo,
                )
            await self.pexaminekey.send(
                f"{self.digest_type}-s{size}--{digest} {filename}\n"
            )
            ### TODO: Do something if readline() returns "" (signalling EOF)
            return (await self.pexaminekey.readline()).strip()

    async def get_key_remotes(self, key: str) -> Optional[List[str]]:
        # Returns None if key is not known to git-annex
        async with self.locks["whereis"]:
            if self.pwhereis is None:
                self.pwhereis = await open_git_annex(
                    self.nursery,
                    "whereis",
                    "--batch-keys",
                    "--json",
                    "--json-error-messages",
                    path=self.repo,
                )
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
        async with self.locks["registerurl"]:
            if self.pregisterurl is None:
                self.pregisterurl = await open_git_annex(
                    self.nursery,
                    "registerurl",
                    "--batch",
                    "--json",
                    "--json-error-messages",
                    path=self.repo,
                )
            await self.pregisterurl.send(f"{key} {url}\n")
            ### TODO: Do something if readline() returns "" (signalling EOF)
            r = json.loads(await self.pregisterurl.readline())
        if not r["success"]:
            log.error(
                "`git annex registerurl %s %s` call failed:%s",
                key,
                url,
                format_errors(r["error-messages"]),
            )
            ### TODO: Raise an exception?

    async def list_files(self) -> AsyncIterator[str]:
        async with await anyio.open_process(
            ["git", "ls-tree", "-r", "--name-only", "-z", "HEAD"],
            cwd=self.repo,
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
                        yield buff + text[:i]
                        buff = ""
                        text = text[i + 1 :]
            if buff:
                yield buff
            ### TODO: Raise an exception if p.returncode is nonzero?
