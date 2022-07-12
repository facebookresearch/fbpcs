# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from typing import Any, Callable, Iterable, Optional, TypeVar

from fbpcs.common.entity.dataclasses_hooks import DataclassHook, HookEventType

T = TypeVar("T")


class UpdateGenericHook(DataclassHook[T]):
    def __init__(
        self,
        update_function: Callable[[T], Any],
        update_condition: Optional[Callable[[T], bool]] = None,
        only_trigger_on_change: bool = True,
        triggers: Optional[Iterable[HookEventType]] = None,
    ) -> None:
        pass

    def run(
        self,
        instance: T,
        field_name: str,
        previous_field_value: Any,
        new_field_value: Any,
        hook_event: HookEventType,
    ) -> None:
        pass

    @property
    def triggers(self) -> Iterable[HookEventType]:
        return self._triggers
