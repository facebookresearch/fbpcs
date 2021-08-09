#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import pathlib
import sys
from typing import List, Optional


ENCODING = sys.getdefaultencoding()


async def _read_stream(
    stream: Optional[asyncio.StreamReader], preamble: str, logger: logging.Logger
) -> None:
    """
    This is a utility method for reading an `asyncio.StreamReader` into a
    given logger. It will run infinitely until the stream is invalidated or
    has run out of data to send.
    """
    logger.debug("Listening to a new StreamReader")
    while True:
        if not stream:
            break
        line = await stream.readline()
        if not line:
            break
        logger.info(f"{preamble}: {line.decode(ENCODING).strip()}")


async def run_command(
    command: List[str], operating_dir: pathlib.Path, logger: logging.Logger
) -> asyncio.subprocess.Process:
    """
    This is a utility method for running a subprocess command. Both `stdout`
    and `stderr` will be redirected to the given logger.
    """
    logger.info("Running new subprocess command")
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=operating_dir,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    logger.info(f"Started subprocess `{' '.join(command)}` (pid={process.pid})")
    try:
        while process.returncode is None:
            await asyncio.wait(
                [
                    _read_stream(process.stdout, "stdout", logger),
                    _read_stream(process.stderr, "stderr", logger),
                ]
            )
    finally:
        if process.returncode is None:
            logger.warning(f"Killing process: {process}")
            process.terminate()

    logger.info(f"pid={process.pid} exited with return code {process.returncode}")
    return process


async def run_commands(
    commands: List[List[str]],
    operating_dirs: List[pathlib.Path],
    loggers: List[logging.Logger],
    timeout: Optional[int] = None,
) -> List[asyncio.subprocess.Process]:
    """
    This is a utility method that runs a set of commands by internally
    calling `run_command` in a loop.
    """
    procs = [
        run_command(command, operating_dir, logger)
        for command, operating_dir, logger in zip(commands, operating_dirs, loggers)
    ]
    tasks = [asyncio.create_task(proc) for proc in procs]
    waits = [asyncio.wait_for(t, timeout=timeout) for t in tasks]
    return list(await asyncio.gather(*waits, return_exceptions=False))
