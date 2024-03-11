#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from dataclasses import dataclass
from typing import List, TypeVar
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

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
class BoltTestHookArgs(BoltHookArgs):
    state: List[int]


class BoltTestHook(BoltHook[BoltTestHookArgs]):
    @bolt_checkpoint()
    async def _inject(self, injection_args: BoltHookCommonInjectionArgs[T, U]) -> None:
        self.hook_args.state.append(0)


class TestBoltHook(IsolatedAsyncioTestCase):
    def test_should_inject_max_injections(self) -> None:
        max_injections = 2
        test_hook = BoltTestHook(
            BoltTestHookArgs(state=[]),
            BoltHookInjectionFrequencyArgs(maximum_injections=max_injections),
        )
        for num_injections in range(max_injections + 2):
            with self.subTest(num_injections=num_injections):
                test_hook._num_injections = num_injections
                self.assertEqual(
                    num_injections < max_injections, test_hook._should_inject()
                )

    def test_should_inject_every_n(self) -> None:
        inject_every_n = 3
        test_hook = BoltTestHook(
            BoltTestHookArgs(state=[]),
            BoltHookInjectionFrequencyArgs(inject_every_n=inject_every_n),
        )
        for num_calls in range(10):
            with self.subTest(num_calls=num_calls):
                test_hook._num_calls = num_calls
                self.assertEqual(
                    num_calls % inject_every_n == 1, test_hook._should_inject()
                )

    @patch("random.random")
    def test_should_inject_with_probability(self, mock_random: Mock) -> None:
        test_hook = BoltTestHook(
            BoltTestHookArgs(state=[]),
            BoltHookInjectionFrequencyArgs(inject_with_probability_p=0.7),
        )

        for random_val in (0.0, 0.134, 0.68):
            with self.subTest(random_val=random_val):
                mock_random.return_value = random_val
                self.assertTrue(test_hook._should_inject())

        random_val = 0.75
        with self.subTest(random_val=random_val):
            mock_random.return_value = random_val
            self.assertFalse(test_hook._should_inject())

    async def test_inject(self) -> None:
        state = []
        test_hook = BoltTestHook(
            BoltTestHookArgs(state=state),
            BoltHookInjectionFrequencyArgs(inject_every_n=2),
        )

        self.assertEqual(0, test_hook._num_calls)
        self.assertEqual(0, test_hook._num_injections)
        self.assertEqual(state, [])

        await test_hook.inject(injection_args=AsyncMock())
        self.assertEqual(1, test_hook._num_calls)
        self.assertEqual(1, test_hook._num_injections)
        self.assertEqual(state, [0])

        await test_hook.inject(injection_args=AsyncMock())
        self.assertEqual(2, test_hook._num_calls)
        self.assertEqual(1, test_hook._num_injections)
        self.assertEqual(state, [0])

        await test_hook.inject(injection_args=AsyncMock())
        self.assertEqual(3, test_hook._num_calls)
        self.assertEqual(2, test_hook._num_injections)
        self.assertEqual(state, [0, 0])
