from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Callable, Container, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import logging
import math
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any, Awaitable, Generic, Optional, TypeVar

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream
from anyio.streams.text import TextReceiveStream
import httpx
from linesep import SplitterEmptyError, TerminatedSplitter, get_newline_splitter

from .consts import DEFAULT_WORKERS, GIT_OPTIONS
from .logging import log
from .util import exp_wait

T = TypeVar("T")
InT = TypeVar("InT")
OutT = TypeVar("OutT")

if sys.version_info[:2] >= (3, 10):
    from contextlib import aclosing
else:
    from async_generator import aclosing


@dataclass
class TextProcess(anyio.abc.ObjectStream[str]):
    p: anyio.abc.Process
    stdout: LineReceiveStream
    desc: str
    warn_on_fail: bool = True

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

    async def force_aclose(self, timeout: float = 5) -> None:
        try:
            with anyio.fail_after(timeout):
                await self.aclose()
                return
        except TimeoutError:
            log.debug(
                "Command %s did not terminate in time; sending SIGTERM", self.desc
            )
            self.p.terminate()
            try:
                with anyio.fail_after(timeout):
                    await self.p.wait()
                    log.debug("Command %s successfully terminated", self.desc)
            except TimeoutError:
                log.warning("Command %s did not terminate in time; killing", self.desc)
                self.p.kill()

    async def send(self, s: str) -> None:
        if self.p.returncode is not None:
            raise RuntimeError(
                f"Command {self.desc} suddenly exited with return code"
                f" {self.p.returncode}!"
            )
        assert self.p.stdin is not None
        await self.p.stdin.send(s.encode("utf-8"))

    async def receive(self) -> str:
        return await self.stdout.receive()

    async def send_eof(self) -> None:
        if self.p.stdin is not None:
            await self.p.stdin.aclose()


async def open_git_annex(
    *args: str,
    path: Optional[Path] = None,
    warn_on_fail: bool = True,
) -> TextProcess:
    # This is strictly for spawning git-annex processes that data will be both
    # sent to and received from.  To open a process solely for receiving data,
    # use `stream_lines_command()` or `stream_null_command()`.
    allargs = ["git", *GIT_OPTIONS, "annex", *args]
    desc = f"`{shlex.join(allargs)}`"
    if path is not None:
        desc += f" [cwd={path}]"
    log.debug("Opening pipe to %s", desc)
    p = await anyio.open_process(
        allargs,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
        cwd=path,
    )
    assert p.stdout is not None
    stdout = LineReceiveStream(TextReceiveStream(p.stdout))
    return TextProcess(p, stdout, desc, warn_on_fail=warn_on_fail)


async def arequest(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    retry_on: Container[int] = (),
    **kwargs: Any,
) -> httpx.Response:
    waits = exp_wait(attempts=15, base=2)
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
    async with kill_on_error(
        await anyio.open_process(argstrs, cwd=cwd, stderr=None), desc
    ) as p:
        assert p.stdout is not None
        try:
            stream = TextReceiveStream(p.stdout)
            splitter = TerminatedSplitter("\0", retain=False)
            async for chunk in splitter.aitersplit(stream):
                yield chunk
        except BaseException:
            log.exception("Exception raised while handling output from %s", desc)
            raise
    log.log(
        logging.DEBUG if p.returncode == 0 else logging.WARNING,
        "Command %s exited with return code %d",
        desc,
        p.returncode,
    )
    ### TODO: Raise an exception if p.returncode is nonzero?


async def stream_lines_command(
    *args: str | Path, cwd: Optional[Path] = None
) -> AsyncGenerator[str, None]:
    argstrs = [str(a) for a in args]
    desc = f"`{shlex.join(argstrs)}`"
    if cwd is not None:
        desc += f" [cwd={cwd}]"
    log.debug("Opening pipe to %s", desc)
    async with kill_on_error(
        await anyio.open_process(argstrs, cwd=cwd, stderr=None), desc
    ) as p:
        assert p.stdout is not None
        async for line in LineReceiveStream(TextReceiveStream(p.stdout)):
            yield line
    log.log(
        logging.DEBUG if p.returncode == 0 else logging.WARNING,
        "Command %s exited with return code %d",
        desc,
        p.returncode,
    )
    ### TODO: Raise an exception if p.returncode is nonzero?


@asynccontextmanager
async def kill_on_error(
    p: anyio.abc.Process, desc: str, timeout: float = 5
) -> AsyncIterator[anyio.abc.Process]:
    """
    When used like so::

        async with kill_on_error(
            await anyio.open_process(...),
            "command args ...",
            timeout=...
        ) as p:
            ...

    then the subprocess ``p``, in addition to being waited for on normal
    context manager exit, will be terminated if an error (including
    cancellation) occurs in the body of the ``async with:`` block; if it
    doesn't exit after ``timeout`` seconds, it will instead be killed.
    """

    async with p:
        try:
            yield p
        except BaseException:
            with anyio.CancelScope(shield=True):
                log.debug("Forcing command %s to terminate", desc)
                p.terminate()
                try:
                    with anyio.fail_after(timeout):
                        await p.wait()
                        log.debug("Command %s successfully terminated", desc)
                except TimeoutError:
                    log.warning("Command %s did not terminate in time; killing", desc)
                    p.kill()
            raise


class LineReceiveStream(anyio.abc.ObjectReceiveStream[str]):
    """
    Stream wrapper that splits strings from ``transport_stream`` on newlines
    and returns each line individually.  Requires the linesep_ package.

    .. _linesep: https://github.com/jwodder/linesep
    """

    def __init__(
        self,
        transport_stream: anyio.abc.ObjectReceiveStream[str],
        newline: Optional[str] = None,
    ) -> None:
        """
        :param transport_stream: any `str`-based receive stream
        :param newline:
            controls how universal newlines mode works; has the same set of
            allowed values and semantics as the ``newline`` argument to
            `open()`
        """
        self._stream = transport_stream
        self._splitter = get_newline_splitter(newline, retain=True)

    async def receive(self) -> str:
        while not self._splitter.nonempty and not self._splitter.closed:
            try:
                self._splitter.feed(await self._stream.receive())
            except anyio.EndOfStream:
                self._splitter.close()
        try:
            return self._splitter.get()
        except SplitterEmptyError:
            raise anyio.EndOfStream()

    async def aclose(self) -> None:
        await self._stream.aclose()

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._stream.extra_attributes
