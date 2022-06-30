# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionConfig,
    AttributionRule,
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)


class TestProductConfig(unittest.TestCase):
    def test_valid_initialization_lift(self) -> None:
        # create product_config
        common: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
        )
        product_Config: ProductConfig = LiftConfig(
            common=common,
        )
        self.assertIsInstance(product_Config, LiftConfig)

    def test_valid_initialization_attribution(self) -> None:
        # create product_config
        common: CommonProductConfig = CommonProductConfig(
            input_path="456",
            output_dir="789",
        )
        product_Config: ProductConfig = AttributionConfig(
            common=common,
            attribution_rule=AttributionRule.LAST_CLICK_1D,
            aggregation_type=AggregationType.MEASUREMENT,
        )
        self.assertIsInstance(product_Config, AttributionConfig)
