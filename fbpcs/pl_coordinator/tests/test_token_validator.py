#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import json
from datetime import datetime, timedelta
from typing import Any, Dict
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock

import requests
from fbpcs.common.service.trace_logging_service import TraceLoggingService
from fbpcs.pl_coordinator.bolt_graphapi_client import BoltGraphAPIClient
from fbpcs.pl_coordinator.constants import INSTANCE_SLA
from fbpcs.pl_coordinator.exceptions import GraphAPITokenValidationError
from fbpcs.pl_coordinator.token_validation_rules import TokenValidationRule
from fbpcs.pl_coordinator.token_validator import TokenValidator


class TestTokenValidator(TestCase):
    def setUp(self) -> None:
        self.client = MagicMock(spec=BoltGraphAPIClient)
        self.trace_logger = MagicMock(spec=TraceLoggingService)
        self.validator = TokenValidator(
            self.client, trace_logging_svc=self.trace_logger
        )

    def test_token_common_rules(self) -> None:
        mock_response = {
            "data": {
                "type": "USER",
                "expires_at": int(
                    datetime.timestamp(
                        datetime.now() + timedelta(seconds=INSTANCE_SLA + 100)
                    )
                ),
                "data_access_expires_at": int(
                    datetime.timestamp(
                        datetime.now() + timedelta(seconds=INSTANCE_SLA + 100)
                    )
                ),
                "is_valid": True,
                "scopes": [
                    "ads_management",
                    "ads_read",
                    "business_management",
                    "private_computation_access",
                ],
            }
        }
        with self.subTest("Token match all validation"):
            self.client.reset_mock()
            self.client.get_debug_token_data.return_value = self._get_graph_api_output(
                mock_response
            )
            self.validator.validate_common_rules()

    def test_token_single_common_rule(self) -> None:
        for (
            sub_test_title,
            test_rule,
            debug_data,
            is_valid,
            cause_msg_regex,
        ) in self.get_token_common_test_data():
            with self.subTest(
                sub_test_title,
                test_rule=test_rule,
                debug_data=debug_data,
                is_valid=is_valid,
                cause_msg_regex=cause_msg_regex,
            ):
                self.validator.debug_token_data = None
                self.trace_logger.reset_mock()
                self.client.reset_mock()
                self.client.get_debug_token_data.return_value = (
                    self._get_graph_api_output(debug_data)
                )
                if is_valid:
                    self.validator.validate_rule(test_rule)
                    self.trace_logger.write_checkpoint.assert_not_called()
                else:
                    with self.assertRaises(GraphAPITokenValidationError) as cm:
                        self.validator.validate_rule(test_rule)
                    self.trace_logger.write_checkpoint.assert_called_once()
                    self.assertRegex(str(cm.exception), cause_msg_regex)

    def get_token_common_test_data(self):
        # sub_test_title, test_rule, debug_data, is_valid, cause_msg_regex
        return (
            (
                "Token valid during computation",
                TokenValidationRule.TOKEN_EXPIRY,
                self._gen_debug_data(
                    expires_at=int(
                        datetime.timestamp(
                            datetime.now() + timedelta(seconds=INSTANCE_SLA + 100)
                        )
                    )
                ),
                True,
                "",
            ),
            (
                "Token never expired",
                TokenValidationRule.TOKEN_EXPIRY,
                self._gen_debug_data(expires_at=0),
                True,
                "",
            ),
            (
                "Token never expire data access",
                TokenValidationRule.TOKEN_DATA_ACCESS_EXPIRY,
                self._gen_debug_data(data_access_expires_at=0),
                True,
                "",
            ),
            (
                "Token miss User type",
                TokenValidationRule.TOKEN_USER_TYPE,
                self._gen_debug_data(type=None),
                False,
                "unexpected token user type None; expected: (.+)",
            ),
            (
                "Token is User type",
                TokenValidationRule.TOKEN_USER_TYPE,
                self._gen_debug_data(type="USER"),
                True,
                "",
            ),
            (
                "Token is System User type",
                TokenValidationRule.TOKEN_USER_TYPE,
                self._gen_debug_data(type="SYSTEM_USER"),
                True,
                "",
            ),
            (
                "Token expire soon",
                TokenValidationRule.TOKEN_EXPIRY,
                self._gen_debug_data(
                    expires_at=int(
                        datetime.timestamp(datetime.now() + timedelta(seconds=100))
                    )
                ),
                False,
                "token 'expires_at': [0-9]+ \\(unix time\\). Token is supposed to be valid in next [0-9]+ hours",
            ),
            (
                "Token data access valid during computation",
                TokenValidationRule.TOKEN_DATA_ACCESS_EXPIRY,
                self._gen_debug_data(
                    data_access_expires_at=int(
                        datetime.timestamp(
                            datetime.now() + timedelta(seconds=INSTANCE_SLA + 100)
                        )
                    )
                ),
                True,
                "",
            ),
            (
                "Token data access expire soon",
                TokenValidationRule.TOKEN_DATA_ACCESS_EXPIRY,
                self._gen_debug_data(
                    data_access_expires_at=int(
                        datetime.timestamp(datetime.now() + timedelta(seconds=100))
                    )
                ),
                False,
                "token 'expires_at': [0-9]+ \\(unix time\\). Token is supposed to be valid in next [0-9]+ hours",
            ),
            (
                "Token not valid",
                TokenValidationRule.TOKEN_VALID,
                self._gen_debug_data(is_valid=False),
                False,
                "token is not valid",
            ),
            (
                "Token not meet permission",
                TokenValidationRule.TOKEN_PERMISSIONS,
                self._gen_debug_data(
                    scopes=[
                        "ads_management",
                        "ads_read",
                        "business_management",
                    ]
                ),
                False,
                "permission scopes missing: {'private_computation_access'}",
            ),
        )

    def _gen_debug_data(self, **kwargs) -> Dict[str, Any]:
        mock_response = {"data": {}}
        for k, v in kwargs.items():
            mock_response["data"][k] = v

        return mock_response

    def _get_graph_api_output(self, text: Any) -> requests.Response:
        r = requests.Response()
        r.status_code = 200
        # pyre-ignore
        type(r).text = PropertyMock(return_value=json.dumps(text))

        def json_func(**kwargs) -> Any:
            return text

        r.json = json_func
        return r
