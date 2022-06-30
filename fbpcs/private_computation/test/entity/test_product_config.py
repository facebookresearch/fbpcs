# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from fbpcs.private_computation.entity.product_config import (
    AttributionConfig,
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)


class TestProductConfig(unittest.TestCase):
    def test_valid_initialization_lift(self) -> None:
        # create product_config
        common_product_config: CommonProductConfig = CommonProductConfig()
        product_Config: ProductConfig = LiftConfig(
            common_product_config=common_product_config,
        )
        self.assertIsInstance(product_Config, LiftConfig)

    def test_valid_initialization_attribution(self) -> None:
        # create product_config
        common_product_config: CommonProductConfig = CommonProductConfig()
        product_Config: ProductConfig = AttributionConfig(
            common_product_config=common_product_config,
        )
        self.assertIsInstance(product_Config, AttributionConfig)
