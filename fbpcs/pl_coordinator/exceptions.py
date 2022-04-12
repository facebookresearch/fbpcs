#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from fbpcs.pl_coordinator.constants import FBPCS_GRAPH_API_TOKEN


class OneCommandRunnerBaseException(Exception):
    def __init__(self, msg: str, cause: str, remediation: str) -> None:
        super().__init__(
            "\n".join((msg, f"Cause: {cause}", f"Remediation: {remediation}"))
        )


class PCStudyValidationException(OneCommandRunnerBaseException, RuntimeError):
    def __init__(self, cause: str, remediation: str) -> None:
        super().__init__(
            msg="PCStudyValidationException",
            cause=cause,
            remediation=remediation,
        )


class PCInstanceCalculationException(OneCommandRunnerBaseException, RuntimeError):
    pass


class GraphAPITokenNotFound(OneCommandRunnerBaseException, RuntimeError):
    @classmethod
    def make_error(cls) -> "GraphAPITokenNotFound":
        return cls(
            msg="Graph API token was not provided to private computation script.",
            cause="Graph API token not found in config.yml file or"
            f" {FBPCS_GRAPH_API_TOKEN} environment variable",
            remediation="Put Graph API token in config.yml file or run"
            f" export {FBPCS_GRAPH_API_TOKEN}=YOUR_TOKEN in your terminal",
        )


class IncompatibleStageError(OneCommandRunnerBaseException, RuntimeError):
    @classmethod
    def make_error(
        cls, publisher_stage_name: str, partner_stage_name: str
    ) -> "IncompatibleStageError":
        return cls(
            msg=f"Publisher stage is {publisher_stage_name} but partner stage is {partner_stage_name}.",
            cause="Possible causes include race time error during instance updating, unexpected instance deletion, or differing stageflows",
            remediation="Wait 24 hours for the instance to expire or contact your representative at Meta.",
        )
