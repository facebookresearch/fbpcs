# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Iterable, Optional, TypeVar

from fbpcs.common.entity.dataclasses_hooks import DataclassHook, HookEventType

from fbpcs.common.entity.exceptions import MissingRangeHookError, OutOfRangeHookError

T = TypeVar("T")


class RangeHook(DataclassHook[T]):
    def __init__(
        self,
        field_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        triggers: Optional[Iterable[HookEventType]] = None,
    ) -> None:
        if min_value is None and max_value is None:
            raise MissingRangeHookError(field_name)

        self.min_value: float = min_value or float("-inf")
        self.max_value: float = max_value or float("inf")

        self._triggers: Iterable[HookEventType] = triggers or [
            HookEventType.PRE_INIT,
            HookEventType.PRE_UPDATE,
        ]

    def run(
        self,
        instance: T,
        field_name: str,
        # pyre-ignore Missing parameter annotation [2]
        previous_field_value: Any,
        # pyre-ignore Missing parameter annotation [2]
        new_field_value: Any,
        hook_event: HookEventType,
    ) -> None:
        if new_field_value > self.max_value or new_field_value < self.min_value:
            raise OutOfRangeHookError(
                field_name, new_field_value, self.min_value, self.max_value
            )

    @property
    def triggers(self) -> Iterable[HookEventType]:
        return self._triggers
