#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from typing import List

from fbpmp.pcf.structs import Game, InputColumn, Metric, Role


class GameNotFoundError(KeyError):
    def __init__(self, name: str):
        self.message = f"Game '{name}' not found in ALL_GAMES"


ConversionLift = Game(
    name="conversion_lift",
    base_game="lift",
    extra_args=["--is_conversion_lift=true"],
    input_columns={
        Role.PUBLISHER: [
            InputColumn.id_,
            InputColumn.test_flag,
            InputColumn.opportunity_timestamp,
        ],
        Role.PARTNER: [
            InputColumn.id_,
            InputColumn.event_timestamps,
            InputColumn.values,
            InputColumn.features,
        ],
    },
    output_metrics=[
        Metric.test_conversions,
        Metric.control_conversions,
        Metric.test_sales,
        Metric.control_sales,
        Metric.test_sales_squared,
        Metric.control_sales_squared,
        Metric.test_population,
        Metric.control_population,
    ],
)


ConverterLift = Game(
    name="converter_lift",
    base_game="lift",
    extra_args=["--is_conversion_lift=false"],
    input_columns={
        Role.PUBLISHER: [
            InputColumn.id_,
            InputColumn.test_flag,
            InputColumn.opportunity_timestamp,
        ],
        Role.PARTNER: [
            InputColumn.id_,
            InputColumn.event_timestamp,
            InputColumn.purchase_flag,
        ],
    },
    output_metrics=[
        Metric.test_purchasers,
        Metric.control_purchasers,
        Metric.test_population,
        Metric.control_population,
    ],
)


SecretShareConversionLift = Game(
    name="secret_share_conversion_lift",
    base_game="secret_share_lift",
    extra_args=["--is_conversion_lift=true"],
    input_columns={
        Role.PUBLISHER: [
            InputColumn.id_,
            InputColumn.test_flag,
            InputColumn.opportunity_timestamps,
            InputColumn.event_timestamp,
            InputColumn.value,
            InputColumn.value_squared,
        ],
        Role.PARTNER: [
            InputColumn.id_,
            InputColumn.test_flag,
            InputColumn.opportunity_timestamps,
            InputColumn.event_timestamp,
            InputColumn.value,
            InputColumn.value_squared,
        ],
    },
    output_metrics=[
        Metric.test_conversions,
        Metric.control_conversions,
        Metric.test_sales,
        Metric.control_sales,
        Metric.test_sales_squared,
        Metric.control_sales_squared,
        Metric.test_population,
        Metric.control_population,
    ],
)


SecretShareConverterLift = Game(
    name="secret_share_converter_lift",
    base_game="secret_share_lift",
    extra_args=["--is_conversion_lift=false"],
    input_columns={
        Role.PUBLISHER: [
            InputColumn.id_,
            InputColumn.test_flag,
            InputColumn.opportunity_timestamps,
            InputColumn.event_timestamp,
            InputColumn.purchase_flag,
        ],
        Role.PARTNER: [
            InputColumn.id_,
            InputColumn.test_flag,
            InputColumn.opportunity_timestamps,
            InputColumn.event_timestamp,
            InputColumn.purchase_flag,
        ],
    },
    output_metrics=[
        Metric.test_purchasers,
        Metric.control_purchasers,
        Metric.test_population,
        Metric.control_population,
    ],
)


ALL_GAMES: List[Game] = [
    ConversionLift,
    ConverterLift,
    SecretShareConversionLift,
    SecretShareConverterLift,
]


def get_game_from_str(s: str) -> Game:
    for game in ALL_GAMES:
        if game.name == s:
            return game
    raise GameNotFoundError(s)
