from dataclasses import dataclass, field
from pathlib import Path
import shlex
import subprocess
import sys
from typing import AsyncIterable, AsyncIterator, Generic, Optional, TypeVar, cast

import anyio
import httpx

from .util import exp_wait, log

T = TypeVar("T")

DEEP_DEBUG = 5


if sys.version_info[:2] >= (3, 10):
    # So aiter() can be re-exported without mypy complaining:
    from builtins import aiter as aiter
else:

    def aiter(obj: AsyncIterable[T]) -> AsyncIterator[T]:
        return obj.__aiter__()


@dataclass
class TextProcess(anyio.abc.AsyncResource):
    p: anyio.abc.Process
    name: str
    encoding: str = "utf-8"
    buff: bytes = b""

    async def aclose(self) -> None:
        if self.p.stdin is not None:
            await self.p.stdin.aclose()
        rc = await self.p.wait()
        if rc != 0 and self.name != "whereis":
            log.warning(
                "git-annex %s command exited with return code %d", self.name, rc
            )

    async def send(self, s: str) -> None:
        if self.p.returncode is not None:
            raise RuntimeError(
                f"git-annex {self.name} command suddenly exited with return"
                f" code {self.p.returncode}!"
            )
        assert self.p.stdin is not None
        log.log(DEEP_DEBUG, "Sending to %s command: %r", self.name, s)
        await self.p.stdin.send(s.encode(self.encoding))

    async def readline(self) -> str:
        if self.p.returncode is not None:
            raise RuntimeError(
                f"git-annex {self.name} command suddenly exited with return"
                f" code {self.p.returncode}!"
            )
        assert self.p.stdout is not None
        while True:
            try:
                i = self.buff.index(b"\n")
            except ValueError:
                try:
                    blob = await self.p.stdout.receive()
                except anyio.EndOfStream:
                    # EOF
                    log.log(DEEP_DEBUG, "%s command reached EOF", self.name)
                    line = self.buff.decode(self.encoding)
                    self.buff = b""
                    log.log(
                        DEEP_DEBUG, "Decoded line from %s command: %r", self.name, line
                    )
                    return line
                else:
                    self.buff += blob
            else:
                line = self.buff[: i + 1].decode(self.encoding)
                self.buff = self.buff[i + 1 :]
                log.log(DEEP_DEBUG, "Decoded line from %s command: %r", self.name, line)
                return line

    async def __aiter__(self) -> AsyncIterator[str]:
        while True:
            line = await self.readline()
            if line == "":
                break
            else:
                yield line


async def open_git_annex(
    _nursery: anyio.abc.TaskGroup, *args: str, path: Optional[Path] = None
) -> TextProcess:
    # The `nursery` argument was necessary when using trio 0.20 and may become
    # necessary in a future version of anyio.
    log.debug("Running git-annex %s", shlex.join(args))
    p = await anyio.open_process(
        ["git-annex", *args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        cwd=path,
    )
    return TextProcess(p, name=args[0])


async def arequest(client: httpx.AsyncClient, method: str, url: str) -> httpx.Response:
    waits = exp_wait(attempts=10, base=1.8)
    while True:
        try:
            r = await client.request(method, url, follow_redirects=True)
            r.raise_for_status()
        except httpx.HTTPError as e:
            if isinstance(e, httpx.RequestError) or (
                isinstance(e, httpx.HTTPStatusError) and e.response.status_code >= 500
            ):
                try:
                    delay = next(waits)
                except StopIteration:
                    raise e
                log.warning(
                    "Retrying %s request to %s in %f seconds as it raised %s: %s",
                    method.upper(),
                    url,
                    delay,
                    type(e).__name__,
                    str(e),
                )
                await anyio.sleep(delay)
                continue
            else:
                raise
        return r


@dataclass
class MiniFuture(Generic[T]):
    event: anyio.Event = field(default_factory=anyio.Event)
    value: Optional[T] = None

    def set(self, value: T) -> None:
        self.value = value
        self.event.set()

    async def get(self) -> T:
        await self.event.wait()
        return cast(T, self.value)
