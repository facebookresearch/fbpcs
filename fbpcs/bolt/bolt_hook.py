#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

# postpone evaluation of type hint annotations until runtime (support forward reference)
# This will become the default in python 4.0: https://peps.python.org/pep-0563/
from __future__ import annotations

import abc
import asyncio
import logging
import random

from dataclasses import dataclass
from typing import Generic, Optional, TYPE_CHECKING, TypeVar

from dataclasses_json import DataClassJsonMixin

# only do these imports when type checking (support forward reference)
if TYPE_CHECKING:
    from fbpcs.bolt.bolt_client import BoltClient
    from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs, BoltJob


@dataclass
class BoltHookInjectionFrequencyArgs:
    """This class is used by the BoltHook interface to modify the behavior of
    arbitrary hooks, e.g. delaying when their execution begins or defining the
    probability with which they trigger.

    These should be kept "private" to the BoltHook interface, meaning implementers
    of the BoltHook interface don't need to worry about managing this behavior.
    """

    delay: Optional[float] = None
    inject_every_n: Optional[int] = None
    maximum_injections: Optional[int] = None
    # this must be between 0 and 1 (inclusive)
    inject_with_probability_p: Optional[float] = None

    def __post_init__(self) -> None:
        if self.inject_every_n and self.inject_with_probability_p:
            raise ValueError(
                "You cannot set both inject_every_n and inject_with_probability_p"
            )

        probability = self.inject_with_probability_p or 0
        if probability < 0 or probability > 1:
            raise ValueError(
                f"{self.inject_with_probability_p=} must be between 0 and 1 (inclusive)"
            )


@dataclass
class BoltHookArgs(DataClassJsonMixin):
    """Hook specific args, as defined by implementers of the BoltHook interface."""

    pass


T = TypeVar("T", bound="BoltCreateInstanceArgs")
U = TypeVar("U", bound="BoltCreateInstanceArgs")


@dataclass
class BoltHookCommonInjectionArgs(Generic[T, U]):
    """Common args intended to be used by implementers of the BoltHook interface"""

    job: "BoltJob[T, U]"
    publisher_client: "BoltClient[T]"
    partner_client: "BoltClient[U]"

    @property
    def publisher_id(self) -> str:
        return self.job.publisher_bolt_args.create_instance_args.instance_id

    @property
    def partner_id(self) -> str:
        return self.job.partner_bolt_args.create_instance_args.instance_id


H = TypeVar("H", bound=BoltHookArgs)


class BoltHook(abc.ABC, Generic[H]):
    """Interface for injecting arbitrary behavior (such as failures) into the BoltRunner.

    hooks_args: Hook specific args, as defined by implementers of the BoltHook interface
    hook_injection_frequency_args: Modify the frequency/cadence at which hooks execute.
        Intended to be insisible to implementers of the BoltHook interface.
    """

    def __init__(
        self,
        hook_args: H,
        hook_injection_frequency_args: Optional[BoltHookInjectionFrequencyArgs] = None,
    ) -> None:
        self.hook_args = hook_args
        self._injection_frequency_args: BoltHookInjectionFrequencyArgs = (
            hook_injection_frequency_args or self._default_frequency_args
        )

        self._num_calls: int = 0
        self._num_injections: int = 0

        self.logger: logging.Logger = logging.getLogger(f"BoltHook_{self.hook_name}")

    @abc.abstractmethod
    async def _inject(
        self,
        injection_args: BoltHookCommonInjectionArgs[T, U],
    ) -> None:
        """Defines the behavior / purpose of the Hook. This must be implemented by
        each BoltHook subclass.

        Arguments:
            injection_args: Arguments passed by the BoltRunner that are used by the
                hook to perform various actions
        """
        ...

    async def inject(self, injection_args: BoltHookCommonInjectionArgs[T, U]) -> None:
        """Inject the hook behavior into the private computation run.

        Note that, depending on the settings provided in BoltHookInjectionFrequencyArgs,
        the hook may not execute every time.

        Arguments:
            injection_args: Arguments passed by the BoltRunner that are used by the
                hook to perform various actions
        """
        self._num_calls += 1
        if not self._should_inject():
            return

        await self._delay()
        self.logger.info(
            f"Running {self.hook_name} on job {injection_args.job.job_name} with"
            f" {self.hook_args=}"
        )
        self._num_injections += 1

        await self._inject(injection_args)

    async def _delay(self) -> None:
        """If a hook delay is configured, async sleep prior to executing hook"""

        if delay := self._injection_frequency_args.delay:
            self.logger.info(f"Waiting {delay} seconds before running {self.hook_name}")
            await asyncio.sleep(delay)

    def _should_inject(self) -> bool:
        """Logic to determine if the hook should be injected or skip injection."""

        if max_injections := self._injection_frequency_args.maximum_injections:
            if self._num_injections >= max_injections:
                self.logger.info(f"Skipping {self.hook_name}: max injections surpassed")
                return False

        if inject_every_n := self._injection_frequency_args.inject_every_n:
            if self._num_calls % inject_every_n != 1:
                self.logger.info(
                    f"Skipping {self.hook_name}: only inject every {inject_every_n} calls"
                )
                return False

        if p := self._injection_frequency_args.inject_with_probability_p:
            if random.random() > p:
                self.logger.info(
                    f"Skipping {self.hook_name}: only inject with probability {p}"
                )
                return False

        return True

    @property
    def hook_name(self) -> str:
        return self.__class__.__name__

    @property
    def _default_frequency_args(self) -> BoltHookInjectionFrequencyArgs:
        """Define the default frequency args for the hook. This allows subclasses
        to define sane defaults and reduce user/caller burden.
        """
        return BoltHookInjectionFrequencyArgs()
