#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum

from fbpcs.pl_coordinator.constants import FBPCS_GRAPH_API_TOKEN
from fbpcs.private_computation.entity.pcs_tier import PCSTier


class OneCommandRunnerExitCode(Enum):
    """Custom exit codes for one command runner

    Unix exit codes 1-2, 126-165, and 255 have special meaning, so they should be
    avoided in this enum.

    We will use exit codes 64-113 to conform to C/C++ standard. If we need more than
    50 exit codes, try not to clash with the unix standard reserved exit codes.

    Resource: https://tldp.org/LDP/abs/html/exitcodes.html
    """

    # Success, in compliance with unix exit code standards
    SUCCESS = 0
    # Catchall for general errors, in compliance with unix exit code standards
    ERROR = 1
    INCORRECT_TIER = 64
    # various incorrect tier exit codes will be used by callers to initiate auto retry
    # when the tier passed to PCS is incorrect
    INCORRECT_TIER_EXPECTED_RC = 65
    INCORRECT_TIER_EXPECTED_CANARY = 66
    INCORRECT_TIER_EXPECTED_LATEST = 67


class OneCommandRunnerBaseException(Exception):
    def __init__(
        self,
        msg: str,
        cause: str,
        remediation: str,
        exit_code: OneCommandRunnerExitCode = OneCommandRunnerExitCode.ERROR,
    ) -> None:
        super().__init__(
            "\n".join(
                (
                    msg,
                    f"Cause: {cause}",
                    f"Remediation: {remediation}",
                    f"Exit code: {exit_code} ({exit_code.value})",
                )
            )
        )
        self.exit_code: OneCommandRunnerExitCode = exit_code


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


class IncorrectVersionError(OneCommandRunnerBaseException, ValueError):
    @classmethod
    def make_error(
        cls, instance_id: str, expected_tier: PCSTier, actual_tier: PCSTier
    ) -> "IncorrectVersionError":
        return cls(
            msg=f"Expected version for instance {instance_id} is {expected_tier.value}"
            f" but the computation was attempted with {actual_tier.value}.",
            cause="The binary_version parameter in your config.yml is incorrect",
            remediation="If using run_fbpcs.sh, the script will auto retry."
            " If you see this message but the computation continued anyway, you"
            " can ignore it. If the computation did not auto retry, you can"
            f" manually pass -- --version={expected_tier.value} to end of the command."
            " If you are not using run_fbpcs.sh, you should update the binary_version"
            f" field in your config.yml to be binary_version: {expected_tier.value}",
            exit_code=cls._determine_exit_code(expected_tier),
        )

    @classmethod
    def _determine_exit_code(cls, expected_tier: PCSTier) -> OneCommandRunnerExitCode:
        if expected_tier is PCSTier.RC:
            return OneCommandRunnerExitCode.INCORRECT_TIER_EXPECTED_RC
        elif expected_tier is PCSTier.CANARY:
            return OneCommandRunnerExitCode.INCORRECT_TIER_EXPECTED_CANARY
        elif expected_tier is PCSTier.PROD:
            return OneCommandRunnerExitCode.INCORRECT_TIER_EXPECTED_LATEST
        else:
            return OneCommandRunnerExitCode.INCORRECT_TIER
