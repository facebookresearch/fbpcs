# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from typing import Any, Callable, Iterable, Optional, TypeVar

from fbpcs.common.entity.dataclasses_hooks import HookEventType
from fbpcs.common.entity.update_generic_hook import UpdateGenericHook

T = TypeVar("T")


class UpdateOtherFieldHook(UpdateGenericHook):
    def __init__(
        self,
        other_field: str,
        update_function: Callable[[T], Any],
        update_condition: Optional[Callable[[T], bool]] = None,
        only_trigger_on_change: bool = True,
        triggers: Optional[Iterable[HookEventType]] = None,
    ) -> None:
        super().__init__(
            update_function=lambda obj: setattr(obj, other_field, update_function(obj)),
            update_condition=update_condition,
            only_trigger_on_change=only_trigger_on_change,
            triggers=triggers,
        )
