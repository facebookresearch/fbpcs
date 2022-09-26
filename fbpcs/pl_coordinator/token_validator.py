#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import time
from typing import Optional, Tuple

from fbpcs.pl_coordinator.constants import INSTANCE_SLA

from fbpcs.pl_coordinator.exceptions import GraphAPITokenValidationError

from fbpcs.pl_coordinator.pc_graphapi_utils import PCGraphAPIClient

from fbpcs.pl_coordinator.token_validation_rules import (
    DebugTokenData,
    TokenValidationRule,
    TokenValidationRuleType,
)


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
COMMON_RULES: Tuple[TokenValidationRule, ...] = (
    TokenValidationRule.TOKEN_USER_TYPE,
    TokenValidationRule.TOKEN_VALID,
    TokenValidationRule.TOKEN_EXPIRY,
    TokenValidationRule.TOKEN_DATA_ACCESS_EXPIRY,
    TokenValidationRule.TOKEN_PERMISSIONS,
)


class TokenValidator:
    def __init__(self, client: PCGraphAPIClient) -> None:
        self.client = client
        self.debug_token_data: Optional[DebugTokenData] = None

    def _load_data(self, rule: TokenValidationRule) -> None:
        if (
            rule.rule_type is TokenValidationRuleType.COMMON
            and self.debug_token_data is None
        ):
            _debug_token_data = json.loads(self.client.get_debug_token_data().text).get(
                "data"
            )
            # pyre-ignore[16]
            self.debug_token_data = DebugTokenData.from_dict(_debug_token_data)

    def validate_common_rules(self) -> None:
        for rule in COMMON_RULES:
            self.validate_rule(rule)

    def validate_rule(self, rule: TokenValidationRule) -> None:
        ## prepare data
        self._load_data(rule=rule)
        if rule is TokenValidationRule.TOKEN_USER_TYPE:
            # pyre-ignore[16]
            if self.debug_token_data.type == "USER":
                return
        elif rule is TokenValidationRule.TOKEN_VALID:
            # pyre-ignore[16]
            if self.debug_token_data.is_valid:
                return
        elif rule is TokenValidationRule.TOKEN_EXPIRY:
            # pyre-ignore[16]
            expires_at = self.debug_token_data.expires_at
            if expires_at == 0 or (
                expires_at > 0 and expires_at - int(time.time()) >= INSTANCE_SLA
            ):  # 24hours
                return
        elif rule is TokenValidationRule.TOKEN_DATA_ACCESS_EXPIRY:
            # pyre-ignore[16]
            expires_at = self.debug_token_data.data_access_expires_at
            if expires_at == 0 or (
                expires_at > 0 and expires_at - int(time.time()) >= INSTANCE_SLA
            ):  # 24hours
                return
        elif rule is TokenValidationRule.TOKEN_PERMISSIONS:
            # pyre-ignore[16]
            scopes = set(self.debug_token_data.scopes)
            if scopes.issuperset(REQUIRED_TOKEN_SCOPES):
                return

        raise GraphAPITokenValidationError.make_error(rule)
