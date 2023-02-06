#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import TypeVar

from fbpcs.bolt.bolt_checkpoint import bolt_checkpoint

from fbpcs.bolt.bolt_hook import (
    BoltHook,
    BoltHookArgs,
    BoltHookCommonInjectionArgs,
    BoltHookInjectionFrequencyArgs,
)
from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs

T = TypeVar("T", bound=BoltCreateInstanceArgs)
U = TypeVar("U", bound=BoltCreateInstanceArgs)


@dataclass
class BoltExceptionThrowerHookArgs(BoltHookArgs):
    exception: BaseException = Exception(
        "Generic exception thrown by BoltExceptionThrowerHook"
    )


class BoltExceptionThrowerHook(BoltHook[BoltExceptionThrowerHookArgs]):
    @bolt_checkpoint()
    async def _inject(self, injection_args: BoltHookCommonInjectionArgs[T, U]) -> None:
        raise self.hook_args.exception

    @property
    def _default_frequency_args(self) -> BoltHookInjectionFrequencyArgs:
        # two second delay, inject exception on every other attempt
        return BoltHookInjectionFrequencyArgs(delay=2, inject_every_n=2)
