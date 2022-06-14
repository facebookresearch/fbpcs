# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from typing import Any, Callable, Iterable, Optional, TypeVar

from fbpcs.common.entity.dataclasses_hooks import DataclassHook, HookEventType

T = TypeVar("T")


class UpdateOtherFieldHook(DataclassHook[T]):
    def __init__(
        self,
        other_field: str,
        update_function: Callable[[T], Any],
        update_condition: Optional[Callable[[T], bool]] = None,
        only_trigger_on_change: bool = True,
        triggers: Optional[Iterable[HookEventType]] = None,
    ) -> None:
        self.other_field = other_field
        self.update_function = update_function
        self.update_condition = update_condition or (lambda _: True)
        self.only_trigger_on_change = only_trigger_on_change
        self._triggers = triggers or [
            HookEventType.POST_INIT,
            HookEventType.POST_UPDATE,
        ]

    def run(
        self,
        instance: T,
        field_name: str,
        previous_field_value: Any,
        new_field_value: Any,
        hook_event: HookEventType,
    ) -> None:
        if (
            previous_field_value != new_field_value or not self.only_trigger_on_change
        ) and self.update_condition(instance):
            setattr(instance, self.other_field, self.update_function(instance))

    @property
    def triggers(self) -> Iterable[HookEventType]:
        return self._triggers
