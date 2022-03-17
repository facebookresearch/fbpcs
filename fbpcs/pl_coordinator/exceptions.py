#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


class OneCommandRunnerBaseException(Exception):
    def __init__(self, msg: str, cause: str, remediation: str) -> None:
        super().__init__(
            "\n".join((msg, f"Cause: {cause}", f"Remediation: {remediation}"))
        )


# TODO(T114624787): [BE][PCS] rename PLInstanceCalculationException to PCInstanceCalculationException
class PLInstanceCalculationException(OneCommandRunnerBaseException, RuntimeError):
    pass


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
