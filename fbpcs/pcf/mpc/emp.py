#!/usr/bin/env/python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import os
import pathlib
import shutil
from typing import Dict, List

from fbpcs.pcf import call_process
from fbpcs.pcf.errors import MPCRuntimeError, MPCStartupError
from fbpcs.pcf.games import (
    ConversionLift,
    ConverterLift,
    SecretShareConversionLift,
    SecretShareConverterLift,
)
from fbpcs.pcf.mpc.base import ServerClientMPCFramework
from fbpcs.pcf.structs import Game, Metric, Status


EMP_GAME_DIR = pathlib.Path(os.environ.get("EMP_GAME_DIR", os.getcwd()))
MAX_ROWS_PER_PARTITION = 1000000  # 1 million


class EmpMPCFramework(ServerClientMPCFramework):
    """
    Implementation of EMP SH2PC MPC Framework
    https://github.com/emp-toolkit/emp-sh2pc
    """

    SUPPORTED_GAMES: List[Game] = [
        ConversionLift,
        ConverterLift,
        SecretShareConversionLift,
        SecretShareConverterLift,
    ]

    async def prepare_input(self) -> Status:
        # We purposefully do not want to use the base class's prepare_input
        # method since it will sort the input which breaks the secret_share
        # game logic (since IDs won't appear to match).
        return Status.OK

    async def run_mpc(self) -> Dict[str, Dict[Metric, int]]:
        """
        Run the MPC game as the given player.
        """
        logger = logging.getLogger(
            f"EmpMPCFramework <Game:{self.game.name}> <{self.player.role!s}>"
        )

        game_path = EMP_GAME_DIR / self.game.base_game
        game_path_absolute = game_path.absolute()

        self._check_executable(game_path_absolute)

        if len(self.other_players) != 0:
            # pre_setup should have validated this, but we put another check
            # here just to reinforce the invariant.
            if len(self.other_players) != 1:
                raise ValueError(
                    f"Must be run with exactly one other player, not {len(self.other_players)}"
                )
            other_player = self.other_players[0]
            ip_address = other_player.ip_address
            port = other_player.port
        else:
            ip_address = self.player.ip_address
            port = self.player.port

        cmd = (
            f"{game_path_absolute} --role={self.player.id}"
            f" --data_directory={self.input_file.parent.absolute()}"
            f" --input_filename={self.input_file.name}"
            f" --server_ip={ip_address}"
            f" --port={port}"
            f" --output_filename={self.output_file}"
        )
        if self.output_s3_path:
            cmd = cmd + f" --output_s3_path={self.output_s3_path}"
        cmd = cmd.split(" ") + self.game.extra_args
        self.base_logger.debug(f"running command: {cmd}")

        try:
            operating_dir = pathlib.Path(os.getcwd())
            result = await asyncio.wait_for(
                call_process.run_command(cmd, operating_dir, logger=logger),
                timeout=self.run_timeout,
            )
        except Exception as e:
            # TODO: Should log e and raise an MPCRuntimeError instead
            raise e

        if result.returncode != 0:
            raise MPCRuntimeError(result.returncode)

        # At this point, assuming everything went correctly, we should have a
        # File with one result per line
        result_filepath = self.input_file.parent / self.output_file
        all_results: Dict[str, Dict[Metric, int]] = {}
        with open(result_filepath) as f:
            for line in f.readlines():
                if len(line) == 0:
                    # For some reason, we sometimes read an empty line from the
                    # output of the EMP MPC program in the result file.
                    continue
                parts = line.strip().split(",")
                feature_group = parts[0]
                contents = [int(field) for field in parts[1:]]
                all_results[feature_group] = {
                    metric: value
                    for metric, value in zip(self.game.output_metrics, contents)
                }
        return all_results

    def _check_executable(self, absolute_path: pathlib.Path) -> None:
        self.base_logger.debug(f"Checking {absolute_path} is executable.")

        if shutil.which(absolute_path) is None:
            raise MPCStartupError(f"Executable {absolute_path} not found.")

    def _check_file_exists(self, absolute_path: pathlib.Path) -> None:
        self.base_logger.debug(f"Checking {absolute_path} exists.")

        if not os.path.isfile(absolute_path):
            raise MPCStartupError(f"File {absolute_path} not found.")

    @staticmethod
    def get_max_rows_per_partition() -> int:
        return MAX_ROWS_PER_PARTITION
