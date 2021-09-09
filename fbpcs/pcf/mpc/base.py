#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import abc
import csv
import logging
import pathlib
from typing import Dict, List, Optional

from fbpcs.pcf.errors import (
    MPCStartupError,
    SetupAlreadyDoneError,
    UnsupportedGameForFrameworkError,
)
from fbpcs.pcf.structs import Game, InputColumn, Metric, Player, Role, Status


class MPCFramework(abc.ABC):
    """
    This class represents an abstract MPCFramework. It includes some helper
    methods related to reading process streams from `asyncio.Process` and for
    spawning those processes through `asyncio.create_subprocess_exec`. To make a
    concrete class from this, the subclass must define the `abc.abstractmethod`s
    for `prepare_input` and `run_mpc`.
    """

    # List of supported games for a given framework
    # Subclasses should override this field
    SUPPORTED_GAMES: List[Game] = []

    def __init__(
        self,
        game: Game,
        input_file: pathlib.Path,
        output_file: str,
        player: Player,
        other_players: List[Player],
        connect_timeout: int,
        run_timeout: int,
        output_s3_path: Optional[str] = None,
        log_level: int = logging.INFO,
    ):
        if not self.supports_game(game):
            raise UnsupportedGameForFrameworkError(self, game)
        self.game = game
        self.input_file = input_file
        self.output_file = output_file
        self.output_s3_path = output_s3_path
        self.player = player
        self.other_players = other_players
        self.connect_timeout = connect_timeout
        self.run_timeout = run_timeout
        self.base_logger = logging.getLogger(f"{self.__class__.__name__} base")
        self.base_logger.setLevel(log_level)
        self.base_logger.info("Calling pre_setup")
        self.__setup_done = False
        self.pre_setup()
        self.__setup_done = True

    def pre_setup(self) -> None:
        """
        This method is called within __init__ for any setup that needs to occur
        before anything else in an MPCFramework. Subclasses may choose to extend
        its functionality.
        """
        if self.__setup_done:
            self.base_logger.error("pre_setup was erroneously called twice")
            raise SetupAlreadyDoneError()

    @classmethod
    def supports_game(cls, game: Game) -> bool:
        """
        Returns whether or not this MPCFramework supports the given game
        """
        return game in cls.SUPPORTED_GAMES

    async def prepare_input(self) -> Status:
        """
        Method that will be called to prepare input for an MPCFramework.
        This function takes `self.input_file` (given as a CSV) and converts it
        as necessary into a usable format for the framework.
        """

        with open(self.input_file) as fin:
            reader = csv.DictReader(fin)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise ValueError("fieldnames is None from csv reader")
            for col in self.game.input_columns[self.player.role]:
                # Don't look for a literal column labeled "features"
                if col != InputColumn.features and str(col) not in fieldnames:
                    raise MPCStartupError(f"{col} column required in input CSV")

        return Status.OK

    @abc.abstractmethod
    async def run_mpc(self) -> Dict[str, Dict[Metric, int]]:
        """
        Abstract method that is called to actually run the MPC program and get
        its results. Results are returned as a map of metrics to their values.
        For example, if a framework writes output data to a CSV, this method
        would be responsible for converting it into a format like so:
        `{Metric.sales_test: 12345.00, Metric.sales_control: 10000.0, ...}`
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def get_max_rows_per_partition() -> int:
        """
        Returns pre-defined, constant max rows per partition
        """
        pass


class TwoPCFramework(MPCFramework):
    """
    This class represents an abstract TwoPCFramework that extends MPCFramework
    in order to support 2PC (two-party computation) only.
    """

    def pre_setup(self) -> None:
        super().pre_setup()
        num_other = len(self.other_players)
        if num_other != 1:
            raise MPCStartupError(
                f"TwoPCFramework only supports 2 players, but got {num_other} other players"
            )


class ServerClientMPCFramework(MPCFramework):
    """
    This class represents an abstract MPCFramework that extends MPCFramework
    in order to support server-client architecture frameworks.
    In these frameworks, the client knows about the server, but the server does
    not need to know anything about the client before starting the computation.
    ASSUMPTION: The PUBLISHER role will always act as the SERVER
    """

    def pre_setup(self) -> None:
        super().pre_setup()
        num_other = len(self.other_players)
        if self.player.role != Role.PUBLISHER and num_other != 1:
            raise MPCStartupError(
                f"ServerClientMPCFramework expects exactly 1 other player for the client, but got {num_other} other players"
            )
