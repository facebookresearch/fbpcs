#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
from dataclasses import dataclass
from typing import Optional, TypeVar

from fbpcs.bolt.bolt_checkpoint import bolt_checkpoint

from fbpcs.bolt.bolt_hook import (
    BoltHook,
    BoltHookArgs,
    BoltHookCommonInjectionArgs,
    BoltHookInjectionFrequencyArgs,
)
from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs
from fbpcs.private_computation.entity.infra_config import PrivateComputationRole

T = TypeVar("T", bound=BoltCreateInstanceArgs)
U = TypeVar("U", bound=BoltCreateInstanceArgs)


@dataclass
class BoltStageCancellerHookArgs(BoltHookArgs):
    role: Optional[PrivateComputationRole] = None


class BoltStageCancellerHook(BoltHook[BoltStageCancellerHookArgs]):
    @bolt_checkpoint()
    async def _inject(self, injection_args: BoltHookCommonInjectionArgs[T, U]) -> None:
        cancel_coros = []
        if self.hook_args.role in (PrivateComputationRole.PUBLISHER, None):
            cancel_coros.append(
                injection_args.publisher_client.cancel_current_stage(
                    injection_args.publisher_id
                )
            )

        if self.hook_args.role in (PrivateComputationRole.PARTNER, None):
            cancel_coros.append(
                injection_args.partner_client.cancel_current_stage(
                    injection_args.partner_id
                )
            )

        await asyncio.gather(*cancel_coros)

    @property
    def _default_frequency_args(self) -> BoltHookInjectionFrequencyArgs:
        # only inject once (one way to avoid cancelling containers every time)
        return BoltHookInjectionFrequencyArgs(maximum_injections=1)
