#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from dataclasses import dataclass, field
from enum import auto, Enum
from typing import List, Optional

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class DebugTokenData:
    type: Optional[str] = field(default=None)
    is_valid: Optional[bool] = field(default=None)
    expires_at: Optional[int] = field(default=None)
    data_access_expires_at: Optional[int] = field(default=None)
    scopes: Optional[List[str]] = field(default=None)


class TokenValidationRuleType(Enum):
    COMMON = auto()
    PRIVATE_LIFT = auto()
    PRIVATE_ATTRIBUTION = auto()


@dataclass(frozen=True)
class TokenValidationRuleData:
    rule_name: str
    rule_type: TokenValidationRuleType


class TokenValidationRule(Enum):
    TOKEN_USER_TYPE = TokenValidationRuleData(
        rule_name="TOKEN_USER_TYPE",
        rule_type=TokenValidationRuleType.COMMON,
    )
    TOKEN_VALID = TokenValidationRuleData(
        rule_name="TOKEN_VALID",
        rule_type=TokenValidationRuleType.COMMON,
    )
    TOKEN_EXPIRY = TokenValidationRuleData(
        rule_name="TOKEN_EXPIRY",
        rule_type=TokenValidationRuleType.COMMON,
    )
    TOKEN_DATA_ACCESS_EXPIRY = TokenValidationRuleData(
        rule_name="TOKEN_DATA_ACCESS_EXPIRY",
        rule_type=TokenValidationRuleType.COMMON,
    )
    TOKEN_PERMISSIONS = TokenValidationRuleData(
        rule_name="TOKEN_PERMISSIONS",
        rule_type=TokenValidationRuleType.COMMON,
    )

    def __init__(self, data: TokenValidationRuleData) -> None:
        super().__init__()
        self.rule_type: TokenValidationRuleType = data.rule_type
