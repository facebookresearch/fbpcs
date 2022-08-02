# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Dict, Optional

from dataclasses_json import dataclass_json, DataClassJsonMixin
from fbpcs.common.entity.dataclasses_hooks import DataclassHookMixin
from fbpcs.common.entity.dataclasses_mutability import (
    DataclassMutabilityMixin,
    immutable_field,
)
from fbpcs.common.entity.frozen_field_hook import FrozenFieldHook
from fbpcs.pid.entity.pid_instance import PIDProtocol
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.post_processing_data import PostProcessingData
from fbpcs.private_computation.service.constants import DEFAULT_PID_PROTOCOL


# This is the visibility defined in https://fburl.com/code/i1itu32l
class ResultVisibility(IntEnum):
    PUBLIC = 0
    PUBLISHER = 1
    PARTNER = 2


@dataclass_json
@dataclass
class CommonProductConfig(DataclassMutabilityMixin):
    """Stores metadata of common product config used both by attribution config and lift config

    Public attributes:
        input_path: the input path of PrivateComputationInstance
        output_dir: the output path of PrivateComputationInstance
        hmac_key: key value of hmac
        padding_size: the id spine combiner would pad each partner row to have this number of conversions.
                        This is required by MPC compute metrics to support multiple conversions per id while
                        at the same time maintaining privacy. It is currently only used when game_type=attribution
                        because the lift id spine combiner uses a hard-coded value of 25.
                        TODO T104391012: pass padding size to lift id spine combiner.
        result_visibility: an enum indicating the visibility of results.
        pid_use_row_numbers: this is used by Private ID protocol to indicate whether we should enable
                                'use-row-numbers' argument.
        multikey_enabled: if it is true, then multiple identifier is used for PID matching; otherwise, only 1 key is used for PID matching.
        pid_protocol: the PIDProtocol that is used for PID matching.
        pid_max_column_count: this specifies how many indentifiers are used for PID matching.
        pid_configs: whether this should be in infra or product is controversial.
        post_processing_data: fields to be sent to the post processing tier.
    """

    # input_path used to be mutable because people always have typo using script
    # Now we have launched computation UI, so input_path can be immutable now
    # TODO: set input_path as immutalbe
    input_path: str
    output_dir: str = immutable_field()

    # TODO T98476320: make the following optional attributes non-optional. They are optional
    # because at the time the instance is created, pl might not provide any or all of them.
    hmac_key: Optional[str] = immutable_field(default=None)
    padding_size: Optional[int] = immutable_field(default=None)

    result_visibility: ResultVisibility = immutable_field(
        default=ResultVisibility.PUBLIC
    )
    pid_use_row_numbers: bool = immutable_field(default=True)
    multikey_enabled: bool = immutable_field(default=True)
    pid_protocol: PIDProtocol = immutable_field(default=DEFAULT_PID_PROTOCOL)
    pid_max_column_count: int = immutable_field(default=1)
    pid_configs: Optional[Dict[str, Any]] = immutable_field(default=None)

    post_processing_data: Optional[PostProcessingData] = immutable_field(default=None)


@dataclass
class ProductConfig(DataClassJsonMixin, DataclassMutabilityMixin):
    """Stores metadata of product config in a private computation instance"""

    common: CommonProductConfig


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

    aggregation_type: AggregationType = immutable_field()
    attribution_rule: AttributionRule = immutable_field(
        default=AttributionRule.LAST_CLICK_1D
    )


# create frozen hook for breakdown_key
frozen_breakdown_key_hook: FrozenFieldHook = FrozenFieldHook(
    other_field="breakdown_key", freeze_when=lambda obj: obj.breakdown_key is not None
)


@dataclass_json
@dataclass
class LiftConfig(ProductConfig):
    """Stores metadata of lift config in product config in a private computation instance

    Public attributes:
        k_anonymity_threshold: Threshold for matched conversions to make results viewable
                                For PA: K-Anon threshold strategy is five clicks per day for an Ad Id.
                                But it will not get the value from PCInstance. Only lift run will use
                                it from PCInstance. So this is a "lift unique" field here.
                                For PL: K-Anon threshold is 100
        breakdown_key: When PL service is running, CreateInstance accepts the breakdown key struct so
                        that the instance can be aware of what cell-objective pair it belongs to at any stage.
    """

    k_anonymity_threshold: int = immutable_field(default=0)

    breakdown_key: Optional[BreakdownKey] = field(
        default=None,
        metadata=DataclassHookMixin.get_metadata(frozen_breakdown_key_hook),
    )
