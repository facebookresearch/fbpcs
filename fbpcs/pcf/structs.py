#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import dataclasses
import enum
import ipaddress
import pathlib
from typing import Dict, List, Union


GAME_DIR = pathlib.Path(__file__) / "game_implementations"


class Role(enum.IntEnum):
    PUBLISHER = 0
    PARTNER = 1

    @classmethod
    def from_str(cls, s: str) -> "Role":
        if s.upper() == "PUBLISHER":
            return cls.PUBLISHER
        elif s.upper() == "PARTNER":
            return cls.PARTNER
        else:
            raise Exception(f"Unknown role: {s}")


class Status(enum.Enum):
    OK = 0
    ERROR = 1


class Metric(enum.Enum):
    id_match_count = 1
    test_conversions = 2
    control_conversions = 3
    test_purchasers = 4
    control_purchasers = 5
    test_sales = 6
    control_sales = 7
    test_sales_squared = 8
    control_sales_squared = 9
    test_population = 10
    control_population = 11
    # Derived metrics below
    scale_factor = 1001
    control_sales_scaled = 1002
    buyers_incremental = 1003
    sales_delta = 1004
    conversions_delta = 1005
    conversions_control_scaled = 1006
    purchasers_control_scaled = 1007
    purchasers_delta = 1008
    conversions_incremental = 1009
    sales_incremental = 1010

    def __str__(self) -> str:
        return self.name

    def __lt__(self, other: "Metric") -> bool:
        return self.name < other.name


class InputColumn(enum.Enum):
    id_ = 1
    opportunity = 2
    test_flag = 3
    opportunity_timestamp = 4
    event_timestamp = 5
    value = 6
    value_squared = 7
    row_count = 8
    purchase_flag = 9
    features = 10
    opportunity_timestamps = 11
    event_timestamps = 12
    values = 13

    @classmethod
    def from_str(cls, s: str) -> "InputColumn":
        if s.startswith("feature_"):
            return cls.features
        return {e.name: e for e in InputColumn}[s]

    def __str__(self) -> str:
        return self.name

    @staticmethod
    def is_feature_str(cls, s: str) -> bool:
        return s.startswith("feature_")


@dataclasses.dataclass(frozen=True)
class Game(object):
    name: str
    base_game: str
    extra_args: List[str] = dataclasses.field(default_factory=list)
    input_columns: Dict[Role, List[InputColumn]] = dataclasses.field(
        default_factory=dict
    )
    output_metrics: List[Metric] = dataclasses.field(default_factory=list)

    @property
    def source_dir(self) -> pathlib.Path:
        return GAME_DIR / self.base_game


@dataclasses.dataclass(frozen=True)
class Player(object):
    role: Role
    ip_address: Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
    port: int

    @property
    def id(self) -> int:
        return int(self.role)

    @classmethod
    def me(cls, role: Role, port: int) -> "Player":
        my_ip = ipaddress.ip_address("127.0.0.1")
        return cls(role, my_ip, port)
