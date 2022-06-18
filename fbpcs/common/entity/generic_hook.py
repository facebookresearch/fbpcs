# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Callable, Iterable, Optional, TypeVar

from fbpcs.common.entity.dataclasses_hooks import DataclassHook, HookEventType

T = TypeVar("T")

"""
This is used for hooks not in (frozen_hook, update_hook or range_hook)
"""


class GenericHook(DataclassHook[T]):
    def __init__(
        self,
        # pyre-ignore Missing parameter annotation [2]
        hook_function: Callable[[T], Any],
        triggers: Iterable[HookEventType],
        hook_condition: Optional[Callable[[T], bool]] = None,
    ) -> None:
        self.hook_function = hook_function
        self.hook_condition: Callable[[T], bool] = hook_condition or (lambda _: True)
        self._triggers = triggers

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
        # if certain condition meets, we call some operations on instance
        if self.hook_condition(instance):
            self.hook_function(instance)

    @property
    def triggers(self) -> Iterable[HookEventType]:
        return self._triggers
