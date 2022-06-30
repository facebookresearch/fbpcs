# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Optional

from dataclasses_json import dataclass_json, DataClassJsonMixin
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey

# This is the visibility defined in https://fburl.com/code/i1itu32l
class ResultVisibility(IntEnum):
    PUBLIC = 0
    PUBLISHER = 1
    PARTNER = 2


@dataclass_json
@dataclass
class CommonProductConfig:
    """Stores metadata of common product config used both by attribution config and lift config

    Public attributes:
        padding_size: the id spine combiner would pad each partner row to have this number of conversions.
                        This is required by MPC compute metrics to support multiple conversions per id while
                        at the same time maintaining privacy. It is currently only used when game_type=attribution
                        because the lift id spine combiner uses a hard-coded value of 25.
                        TODO T104391012: pass padding size to lift id spine combiner.
    """

    input_path: str
    output_dir: str

    # TODO T98476320: make the following optional attributes non-optional. They are optional
    # because at the time the instance is created, pl might not provide any or all of them.
    hmac_key: Optional[str] = None
    padding_size: Optional[int] = None
    result_visibility: ResultVisibility = ResultVisibility.PUBLIC

    # this is used by Private ID protocol to indicate whether we should
    # enable 'use-row-numbers' argument.
    pid_use_row_numbers: bool = True


@dataclass
class ProductConfig(DataClassJsonMixin):
    """Stores metadata of product config in a private computation instance"""

    common_product_config: CommonProductConfig


class AttributionRule(Enum):
    LAST_CLICK_1D = "last_click_1d"
    LAST_CLICK_7D = "last_click_7d"
    LAST_CLICK_28D = "last_click_28d"
    LAST_TOUCH_1D = "last_touch_1d"
    LAST_TOUCH_7D = "last_touch_7d"
    LAST_TOUCH_28D = "last_touch_28d"
    LAST_CLICK_2_7D = "last_click_2_7d"
    LAST_TOUCH_2_7D = "last_touch_2_7d"
    LAST_CLICK_1D_TARGETID = "last_click_1d_targetid"


class AggregationType(Enum):
    MEASUREMENT = "measurement"


@dataclass_json
@dataclass
class AttributionConfig(ProductConfig):
    """Stores metadata of attribution config in product config in a private computation instance

    Public attributes:
        attribution_rule: the rule that a conversion is attributed to an exposure (e.g., last_click_1d,
                            last_click_28d, last_touch_1d, last_touch_28d).
        aggregation_type: the level the statistics are aggregated at (e.g., ad-object, which includes ad,
                            campaign and campaign group). In the future, aggregation_type will also be
                            used to infer the metrics_format_type argument of the shard aggregator game.
    """

    attribution_rule: AttributionRule
    aggregation_type: AggregationType


@dataclass_json
@dataclass
class LiftConfig(ProductConfig):
    """Stores metadata of lift config in product config in a private computation instance

    Public attributes:

    """

    k_anonymity_threshold: int = 0
    breakdown_key: Optional[BreakdownKey] = None
