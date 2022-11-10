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


class TokenRuleException(RuntimeError):
    pass


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

VALID_USER_TYPE = (
    "USER",
    "SYSTEM_USER",
)

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
    rule_checker: Callable[[DebugTokenData], None]


"""
rule checkers
"""


def user_type_checker(data: DebugTokenData) -> None:
    if data.type not in VALID_USER_TYPE:
        raise TokenRuleException(
            f"unexpected token user type {data.type}; expected: {VALID_USER_TYPE}"
        )


def valid_checker(data: DebugTokenData) -> None:
    if not data.is_valid:
        raise TokenRuleException("token is not valid")


def expiry_checker(data: DebugTokenData) -> None:
    expires_at = data.expires_at
    if expires_at is None:
        raise TokenRuleException("token missing 'expires_at' field")

    if expires_at == 0 or (
        expires_at > 0 and expires_at - int(time.time()) >= INSTANCE_SLA
    ):
        return None

    raise TokenRuleException(
        f"token 'expires_at': {expires_at} (unix time). Token is supposed to be valid in next {int(INSTANCE_SLA/3600)} hours."
    )


def data_access_expiry_checker(data: DebugTokenData) -> None:
    expires_at = data.data_access_expires_at
    if expires_at is None:
        raise TokenRuleException("token missing 'expires_at' field")

    if expires_at == 0 or (
        expires_at > 0 and expires_at - int(time.time()) >= INSTANCE_SLA
    ):
        return None

    raise TokenRuleException(
        f"token 'expires_at': {expires_at} (unix time). Token is supposed to be valid in next {int(INSTANCE_SLA/3600)} hours."
    )


def permission_checker(data: DebugTokenData) -> None:
    if data.scopes is None:
        raise TokenRuleException("token missing 'scopes' field")

    missing_perm = REQUIRED_TOKEN_SCOPES - set(data.scopes)
    if missing_perm:
        raise TokenRuleException(f"permission scopes missing: {missing_perm}")


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
        self.rule_checker: Callable[[DebugTokenData], None] = data.rule_checker
