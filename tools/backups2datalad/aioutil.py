from __future__ import annotations

from dataclasses import dataclass, field
import logging
import math
from pathlib import Path
import shlex
import subprocess
import sys
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Container,
    Generic,
    Optional,
    TypeVar,
)

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream
from anyio.streams.text import TextReceiveStream
import httpx

from .consts import DEFAULT_WORKERS
from .logging import log
from .util import exp_wait

T = TypeVar("T")
InT = TypeVar("InT")
OutT = TypeVar("OutT")

DEEP_DEBUG = 5

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class TextProcess(anyio.abc.ObjectStream[str]):
    p: anyio.abc.Process
    desc: str
    warn_on_fail: bool = True
    encoding: str = "utf-8"
    buff: bytes = b""
    done: bool = False

    async def aclose(self) -> None:
        if self.p.stdin is not None:
            await self.p.stdin.aclose()
        log.debug("Waiting for %s to terminate", self.desc)
        rc = await self.p.wait()
        log.log(
            logging.WARNING if rc != 0 and self.warn_on_fail else logging.DEBUG,
            "Command %s exited with return code %d",
            self.desc,
            rc,
        )

    async def send(self, s: str) -> None:
        if self.p.returncode is not None:
            raise RuntimeError(
                f"Command {self.desc} suddenly exited with return code"
                f" {self.p.returncode}!"
            )
        assert self.p.stdin is not None
        log.log(DEEP_DEBUG, "Sending to command %s: %r", self.desc, s)
        await self.p.stdin.send(s.encode(self.encoding))

    async def receive(self) -> str:
        if self.done:
            raise anyio.EndOfStream()
        if self.p.returncode not in (None, 0):
            raise RuntimeError(
                f"Command {self.desc} suddenly exited with return code"
                f" {self.p.returncode}!"
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
                    log.log(DEEP_DEBUG, "Stdout of command %s reached EOF", self.desc)
                    line = self.buff.decode(self.encoding)
                    self.buff = b""
                    log.log(
                        DEEP_DEBUG, "Decoded line from command %s: %r", self.desc, line
                    )
                    self.done = True
                    if line:
                        return line
                    else:
                        raise anyio.EndOfStream()
                else:
                    self.buff += blob
            else:
                line = self.buff[: i + 1].decode(self.encoding)
                self.buff = self.buff[i + 1 :]
                log.log(DEEP_DEBUG, "Decoded line from command %s: %r", self.desc, line)
                return line

    async def send_eof(self) -> None:
        if self.p.stdin is not None:
            await self.p.stdin.aclose()


async def open_git_annex(
    *args: str,
    path: Optional[Path] = None,
    use_stdin: bool = True,
    warn_on_fail: bool = True,
) -> TextProcess:
    desc = f"`git-annex {shlex.join(args)}`"
    if path is not None:
        desc += f" [cwd={path}]"
    log.debug("Opening pipe to %s", desc)
    p = await anyio.open_process(
        ["git-annex", *args],
        stdin=subprocess.PIPE if use_stdin else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=None,
        cwd=path,
    )
    return TextProcess(p, desc, warn_on_fail=warn_on_fail)


async def arequest(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    retry_on: Container[int] = (),
    **kwargs: Any,
) -> httpx.Response:
    waits = exp_wait(attempts=10, base=1.8)
    while True:
        try:
            r = await client.request(method, url, follow_redirects=True, **kwargs)
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
class PoolReport(Generic[InT, OutT]):
    results: list[tuple[InT, OutT]] = field(default_factory=list)
    failed: list[InT] = field(default_factory=list)


async def pool_amap(
    func: Callable[[InT], Awaitable[OutT]],
    inputs: AsyncGenerator[InT, None],
    workers: int = DEFAULT_WORKERS,
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
    # Note: stdout/err will be output as ran and not along with the
    # exception if check was not set to False and command exits with
    # non-0 status leading to CalledProcessError -- hard to associate
    # the output produced by the command (might be an error) with the
    # failed run/exception.
    kwargs.setdefault("stdout", None)
    kwargs.setdefault("stderr", None)
    return await anyio.run_process(argstrs, **kwargs)


async def areadcmd(*args: str | Path, **kwargs: Any) -> str:
    kwargs["stdout"] = subprocess.PIPE
    kwargs.setdefault("stderr", None)
    r = await aruncmd(*args, **kwargs)
    return r.stdout.decode("utf-8").strip()


async def stream_null_command(
    *args: str | Path, cwd: Optional[Path] = None
) -> AsyncGenerator[str, None]:
    argstrs = [str(a) for a in args]
    desc = f"`{shlex.join(argstrs)}`"
    if cwd is not None:
        desc += f" [cwd={cwd}]"
    log.debug("Opening pipe to %s", desc)
    async with await anyio.open_process(argstrs, cwd=cwd, stderr=None) as p:
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
    log.log(
        logging.DEBUG if p.returncode == 0 else logging.WARNING,
        "Command %s exited with return code %d",
        desc,
        p.returncode,
    )
    ### TODO: Raise an exception if p.returncode is nonzero?
