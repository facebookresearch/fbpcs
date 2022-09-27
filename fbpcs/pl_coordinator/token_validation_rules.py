#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import time
from dataclasses import dataclass, field
from enum import auto, Enum
from typing import Callable, List, Optional

from dataclasses_json import dataclass_json

from fbpcs.pl_coordinator.constants import INSTANCE_SLA

"""
required token scopes defined here:
https://github.com/facebookresearch/fbpcs/blob/main/docs/PCS_Partner_Playbook_UI.pdf
(see Step 3: generating 60 days access token)
"""
REQUIRED_TOKEN_SCOPES = {
    "ads_management",
    "ads_read",
    "business_management",
    "private_computation_access",
}


"""
data models define here
"""


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


"""
Token validation rule data model
"""


@dataclass(frozen=True)
class TokenValidationRuleData:
    rule_name: str
    rule_type: TokenValidationRuleType
    rule_checker: Callable[[DebugTokenData], bool]


"""
rule checkers
"""
user_type_checker: Callable[[DebugTokenData], bool] = lambda data: data.type == "USER"
valid_checker: Callable[[DebugTokenData], bool] = lambda data: data.is_valid


def expiry_checker(data: DebugTokenData) -> bool:
    expires_at = data.expires_at
    if expires_at is None:
        return False

    return expires_at == 0 or (
        expires_at > 0 and expires_at - int(time.time()) >= INSTANCE_SLA
    )


def data_access_expiry_checker(data: DebugTokenData) -> bool:
    expires_at = data.data_access_expires_at
    if expires_at is None:
        return False

    return expires_at == 0 or (
        expires_at > 0 and expires_at - int(time.time()) >= INSTANCE_SLA
    )


def permission_checker(data: DebugTokenData) -> bool:
    if data.scopes is None:
        return False

    return set(data.scopes).issuperset(REQUIRED_TOKEN_SCOPES)


"""
Rule definitions
"""


class TokenValidationRule(Enum):
    TOKEN_USER_TYPE = TokenValidationRuleData(
        rule_name="TOKEN_USER_TYPE",
        rule_type=TokenValidationRuleType.COMMON,
        rule_checker=user_type_checker,
    )
    TOKEN_VALID = TokenValidationRuleData(
        rule_name="TOKEN_VALID",
        rule_type=TokenValidationRuleType.COMMON,
        rule_checker=valid_checker,
    )
    TOKEN_EXPIRY = TokenValidationRuleData(
        rule_name="TOKEN_EXPIRY",
        rule_type=TokenValidationRuleType.COMMON,
        rule_checker=expiry_checker,
    )
    TOKEN_DATA_ACCESS_EXPIRY = TokenValidationRuleData(
        rule_name="TOKEN_DATA_ACCESS_EXPIRY",
        rule_type=TokenValidationRuleType.COMMON,
        rule_checker=data_access_expiry_checker,
    )
    TOKEN_PERMISSIONS = TokenValidationRuleData(
        rule_name="TOKEN_PERMISSIONS",
        rule_type=TokenValidationRuleType.COMMON,
        rule_checker=permission_checker,
    )

    def __init__(self, data: TokenValidationRuleData) -> None:
        super().__init__()
        self.rule_type: TokenValidationRuleType = data.rule_type
        self.rule_checker: Callable[[DebugTokenData], bool] = data.rule_checker
