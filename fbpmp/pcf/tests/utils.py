#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import ipaddress
from typing import Dict, List, Optional, Union

import fbpmp.pcf.networking as networking
from fbpmp.pcf.mpc.base import MPCFramework, TwoPCFramework
from fbpmp.pcf.structs import Game, InputColumn, Metric, Player, Role, Status
from fbpmp.pid.entity.structs import PIDPlayer, PIDRole


MAX_ROWS_PER_PARTITION = 10

DummyGame = Game(
    name="dummy_game",
    base_game="lift",
    extra_args=[],
    input_columns={Role.PUBLISHER: [InputColumn.id_], Role.PARTNER: [InputColumn.id_]},
    output_metrics=[Metric.id_match_count],
)


class DummyMPCFramework(MPCFramework):
    def __init__(self, *args, _supports_game=True, **kwargs):
        # Default for _supports_game to avoid exception in __init__
        self._supports_game = _supports_game

        # Then we can call super() and set other properties
        super().__init__(*args, **kwargs)
        self._prepare_input: Status = Status.OK
        self._run_mpc: Optional[Dict[str, Dict[Metric, int]]] = None

    def build(
        self,
        supports_game: bool = False,
        prepare_input: Status = Status.OK,
        run_mpc: Optional[Dict[str, Dict[Metric, int]]] = None,
    ):
        if run_mpc is None:
            run_mpc = {"Overall": {Metric.id_match_count: 123}}

        self._supports_game = supports_game
        self._prepare_input = prepare_input
        self._run_mpc = run_mpc

    # pyre-fixme[14]: `supports_game` overrides method defined in `MPCFramework`
    #  inconsistently.
    def supports_game(self, game: Game) -> bool:
        return self._supports_game

    async def prepare_input(self) -> Status:
        return self._prepare_input

    async def run_mpc(self) -> Dict[str, Dict[Metric, int]]:
        res = self._run_mpc
        assert res is not None
        return res

    @staticmethod
    def get_max_rows_per_partition() -> int:
        return MAX_ROWS_PER_PARTITION


class DummyPlayer(Player):
    @classmethod
    def build(
        cls,
        role: Role = Role.PUBLISHER,
        ip_address: Optional[
            Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
        ] = None,
        port: Optional[int] = None,
    ):
        if ip_address is None:
            ip_address = ipaddress.ip_address("127.0.0.1")

        port = port or networking.find_free_port()

        return cls(role, ip_address, port)


class Dummy2PCFramework(TwoPCFramework):
    SUPPORTED_GAMES: List[Game] = [DummyGame]

    async def prepare_input(self) -> Status:
        return Status.OK

    async def run_mpc(self) -> Dict[str, Dict[Metric, int]]:
        return {}

    @staticmethod
    def get_max_rows_per_partition() -> int:
        return MAX_ROWS_PER_PARTITION


class DummyPIDPlayer(PIDPlayer):
    @classmethod
    def build(
        cls,
        role: PIDRole = PIDRole.PUBLISHER,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
    ):
        if hostname is None:
            hostname = "0.0.0.0"

        port = port or networking.find_free_port()

        return cls(role, hostname, port)
