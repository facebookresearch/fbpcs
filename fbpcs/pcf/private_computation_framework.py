#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import os
import pathlib
from typing import Dict, List, Optional, Type

from fbpcs.pcf.mpc.base import MPCFramework
from fbpcs.pcf.structs import Game, Metric, Player, Role, Status


# PCF constants
DEFAULT_CONNECT_TIMEOUT = 0
DEFAULT_RUN_TIMEOUT = 3600
DEFAULT_SLEEP_SECONDS = 5


class PrivateComputationFramework(object):
    def __init__(
        self,
        game: Game,
        input_files: List[pathlib.Path],
        output_files: List[str],
        player: Player,
        other_players: List[Player],
        mpc_cls: Type[MPCFramework],
        output_s3_path: Optional[str] = None,
        connect_timeout: Optional[int] = None,
        run_timeout: Optional[int] = None,
        log_path: Optional[pathlib.Path] = None,
        log_level: int = logging.INFO,
        partner_sleep_seconds: int = DEFAULT_SLEEP_SECONDS,
    ):
        # First configure the root logger to ensure log messages are output
        if log_path is not None:
            logging.basicConfig(filename=log_path, level=log_level)
        else:
            logging.basicConfig(level=log_level)
        self.logger = logging.getLogger(__name__)
        if connect_timeout is None:
            connect_timeout = int(
                os.environ.get("CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT)
            )
        if run_timeout is None:
            self.logger.debug("Retrieving run_timeout from environment")
            run_timeout = int(os.environ.get("RUN_TIMEOUT", DEFAULT_RUN_TIMEOUT))
        self.logger.info(f"run_timeout={run_timeout}")
        self.player = player
        self.partner_sleep_seconds = partner_sleep_seconds
        self.mpc_frameworks = self._gen_frameworks(
            game=game,
            input_files=input_files,
            output_files=output_files,
            output_s3_path=output_s3_path,
            player=player,
            other_players=other_players,
            mpc_cls=mpc_cls,
            connect_timeout=connect_timeout,
            run_timeout=run_timeout,
        )
        self.logger.info("Done initializing MPC framework.")

    # Helper function for counting the number of lines in the input file
    # Reference: https://stackoverflow.com/a/9631635
    @staticmethod
    def _blocks(files, size=65536):  # 64*1024
        while True:
            b = files.read(size)
            if not b:
                break
            yield b

    @classmethod
    def _gen_frameworks(
        cls,
        game: Game,
        input_files: List[pathlib.Path],
        output_files: List[str],
        output_s3_path: Optional[str],
        player: Player,
        other_players: List[Player],
        mpc_cls: Type[MPCFramework],
        connect_timeout: int,
        run_timeout: int,
    ) -> List[MPCFramework]:
        fws: List[MPCFramework] = []
        for i, file in enumerate(input_files):
            max_rows = int(
                os.environ.get("MAX_ROWS", mpc_cls.get_max_rows_per_partition())
            )
            # count the number of rows in the input file, deduct 1 because of the header line
            with open(file, "r") as f:
                num_rows = sum(bl.count("\n") for bl in cls._blocks(f)) - 1
            if num_rows > max_rows:
                raise AssertionError(
                    f"{file} has {num_rows} rows, but the max num rows allowed is {max_rows}"
                )

            fws.append(
                # pyre-ignore # T68013847
                mpc_cls(
                    game=game,
                    input_file=file,
                    output_file=output_files[i],
                    output_s3_path=output_s3_path,
                    player=player,
                    other_players=other_players,
                    connect_timeout=connect_timeout,
                    run_timeout=run_timeout,
                )
            )

        return fws

    async def prepare_input(self) -> Status:
        self.logger.info("Calling prepare_input")
        for mpc_framework in self.mpc_frameworks:
            status = await mpc_framework.prepare_input()
            if status != Status.OK:
                return status
        return Status.OK

    async def run_mpc(self) -> List[Dict[str, Dict[Metric, int]]]:
        self.logger.info("Calling run_mpc")
        results: List[Dict[str, Dict[Metric, int]]] = []
        for mpc_framework in self.mpc_frameworks:
            if self.player.role != Role.PUBLISHER:
                await asyncio.sleep(self.partner_sleep_seconds)
            result = await mpc_framework.run_mpc()
            results.append(result)
        return results

    # Convenience method that does everything
    async def run(self) -> List[Dict[str, Dict[Metric, int]]]:
        await self.prepare_input()
        return await self.run_mpc()
