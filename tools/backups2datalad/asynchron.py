import asyncio
import json
from pathlib import Path
import shlex
import subprocess
from typing import Iterable, List, Optional, Tuple

from . import log
from .util import quantify


async def download_urls(
    repo_path: Path, urls_paths: Iterable[Tuple[str, Optional[str]]], jobs: int = 10
) -> None:
    args = [
        "git-annex",
        "addurl",
        "--batch",
        "--with-files",
        "--jobs",
        str(jobs),
        "--json",
        "--json-error-messages",
        "--json-progress",
        "--raw",
    ]
    log.debug("Running: %s", shlex.join(args))
    process = await asyncio.create_subprocess_exec(
        *args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        cwd=repo_path,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    input_lines = [f"{url} {filepath}\n" for url, filepath in urls_paths]
    _, failures = await asyncio.gather(
        feed_input(process.stdin, input_lines),
        read_output(process.stdout),
    )
    r = await process.wait()
    if failures:
        raise RuntimeError(f"{quantify(failures, 'asset')} failed to download")
    if r != 0:
        raise RuntimeError(f"git-annex addurl exited with return code {r}")


async def feed_input(fp: asyncio.StreamWriter, lines: List[str]) -> None:
    for ln in lines:
        fp.write(ln.encode("utf-8"))
        await fp.drain()
    fp.close()
    await fp.wait_closed()


async def read_output(fp: asyncio.StreamReader) -> int:
    failures = 0
    async for line in fp:
        data = json.loads(line)
        if "success" not in data:
            # Progress message
            log.info(
                "%s: Downloaded %d / %d bytes (%s)",
                data["action"]["file"],
                data["byte-progress"],
                data["total-size"],
                data["percent-progress"],
            )
        elif not data["success"]:
            log.error(
                "%s: download failed; error messages: %r",
                data["file"],
                data["error-messages"],
            )
            failures += 1
        else:
            key = data.get("key")  # Absent for text files
            log.info("Finished downloading %s (key = %s)", data["file"], key)
    return failures
