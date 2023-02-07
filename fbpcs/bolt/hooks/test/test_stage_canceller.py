#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from fbpcs.bolt.hooks.stage_canceller import (
    BoltStageCancellerHook,
    BoltStageCancellerHookArgs,
)
from fbpcs.private_computation.entity.infra_config import PrivateComputationRole


class TestBoltStageCancellerHook(IsolatedAsyncioTestCase):
    async def test_cancel_stage_partner(self) -> None:
        injection_args = AsyncMock()
        test_hook = BoltStageCancellerHook(
            BoltStageCancellerHookArgs(role=PrivateComputationRole.PARTNER)
        )

        await test_hook._inject(injection_args)

        injection_args.partner_client.cancel_current_stage.assert_called_once()
        injection_args.publisher_client.cancel_current_stage.assert_not_called()

    async def test_cancel_stage_publisher(self) -> None:
        injection_args = AsyncMock()
        test_hook = BoltStageCancellerHook(
            BoltStageCancellerHookArgs(role=PrivateComputationRole.PUBLISHER)
        )

        await test_hook._inject(injection_args)

        injection_args.publisher_client.cancel_current_stage.assert_called_once()
        injection_args.partner_client.cancel_current_stage.assert_not_called()

    async def test_cancel_stage_both(self) -> None:
        injection_args = AsyncMock()
        test_hook = BoltStageCancellerHook(BoltStageCancellerHookArgs(role=None))

        await test_hook._inject(injection_args)

        injection_args.publisher_client.cancel_current_stage.assert_called_once()
        injection_args.partner_client.cancel_current_stage.assert_called_once()
