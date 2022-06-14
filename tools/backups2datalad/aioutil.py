from __future__ import annotations

from dataclasses import dataclass, field
import math
from pathlib import Path
import shlex
import subprocess
import sys
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Container,
    Generic,
    Optional,
    TypeVar,
    cast,
)

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream
import httpx

from .logging import log
from .util import exp_wait

T = TypeVar("T")
InT = TypeVar("InT")
OutT = TypeVar("OutT")

DEEP_DEBUG = 5

if sys.version_info[:2] >= (3, 10):
    # So aiter() can be re-exported without mypy complaining:
    from builtins import aiter as aiter
    from contextlib import aclosing
else:
    from async_generator import aclosing

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


async def open_git_annex(*args: str, path: Optional[Path] = None) -> TextProcess:
    log.debug("Running git-annex %s", shlex.join(args))
    p = await anyio.open_process(
        ["git-annex", *args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
        cwd=path,
    )
    return TextProcess(p, name=args[0])


async def arequest(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    json: Any = None,
    retry_on: Container[int] = (),
) -> httpx.Response:
    waits = exp_wait(attempts=10, base=1.8)
    while True:
        try:
            r = await client.request(method, url, follow_redirects=True, json=json)
            r.raise_for_status()
        except httpx.HTTPError as e:
            if isinstance(e, httpx.RequestError) or (
                isinstance(e, httpx.HTTPStatusError)
                and (
                    e.response.status_code >= 500 or e.response.status_code in retry_on
                )
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


@dataclass
class PoolReport(Generic[InT, OutT]):
    results: list[tuple[InT, OutT]] = field(default_factory=list)
    failed: list[InT] = field(default_factory=list)


async def pool_amap(
    func: Callable[[InT], Awaitable[OutT]],
    inputs: AsyncIterable[InT],
    workers: int = 5,
) -> PoolReport[InT, OutT]:
    report: PoolReport[InT, OutT] = PoolReport()

    async def dowork(rec: MemoryObjectReceiveStream[InT]) -> None:
        async with rec:
            async for inp in rec:
                try:
                    outp = await func(inp)
                except Exception:
                    log.exception("Job failed on input %r:", inp)
                    report.failed.append(inp)
                else:
                    report.results.append((inp, outp))

    async with anyio.create_task_group() as tg:
        sender, receiver = anyio.create_memory_object_stream(math.inf)
        async with receiver:
            for _ in range(max(1, workers)):
                tg.start_soon(dowork, receiver.clone())
        async with sender, aclosing(inputs):
            async for item in inputs:
                await sender.send(item)
    return report


async def aruncmd(
    *args: str | Path, **kwargs: Any
) -> subprocess.CompletedProcess[bytes]:
    argstrs = [str(a) for a in args]
    if (cwd := kwargs.get("cwd")) is not None:
        attrs = f" [cwd={cwd}]"
    else:
        attrs = ""
    log.debug("Running: %s%s", shlex.join(argstrs), attrs)
    kwargs.setdefault("stdout", None)
    kwargs.setdefault("stderr", None)
    return await anyio.run_process(argstrs, **kwargs)


async def areadcmd(*args: str | Path, **kwargs: Any) -> str:
    kwargs["stdout"] = subprocess.PIPE
    kwargs.setdefault("stderr", None)
    r = await aruncmd(*args, **kwargs)
    return r.stdout.decode("utf-8").strip()
