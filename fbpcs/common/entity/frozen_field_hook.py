# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Callable, Iterable, Optional, TypeVar

from fbpcs.common.entity.dataclasses_hooks import (
    DataclassHook,
    DataclassHookMixin,
    HookEventType,
)
from fbpcs.common.entity.dataclasses_mutability import (
    IS_FROZEN_FIELD,
    MutabilityMetadata,
)

T = TypeVar("T")


class FrozenFieldHook(DataclassHook[T]):
    def __init__(
        self, other_field: str, freeze_when: Optional[Callable[[T], bool]] = None
    ) -> None:
        self.other_field: str = other_field
        self.freeze_when: Callable[[T], bool] = freeze_when or (lambda _: True)
        self._triggers: Iterable[HookEventType] = [
            HookEventType.POST_UPDATE,
            HookEventType.POST_DELETE,
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
        if previous_field_value != new_field_value and self.freeze_when(instance):
            self._freeze_field(instance, self.other_field)

    @property
    def triggers(self) -> Iterable[HookEventType]:
        return self._triggers

    def _freeze_field(self, instance: T, field_name: str) -> None:
        """
        This function will freeze the field with field_name in object: instance
        """
        # pyre-ignore Undefined attribute [16]: instance has no attribute __dataclass_fields__
        field_obj = instance.__dataclass_fields__[field_name]

        # if this field is mutable now
        if not field_obj.metadata.get(IS_FROZEN_FIELD, False):
            # get the hooks of this field
            hooks: Iterable[HookEventType] = field_obj.metadata.get(
                DataclassHookMixin.HOOK_METADATA_STR, None
            )

            if hooks is None:
                # No hooks, so just set the metadata as immutable
                field_obj.metadata = MutabilityMetadata.IMMUTABLE.value
            else:
                # if this field has hooks
                # We want to set the metadata as immutable and keep a record
                # of the existing hooks (field metadata can't be updated because
                # it is a "mappingproxy" object, not a real dict)
                field_obj.metadata = {
                    **MutabilityMetadata.IMMUTABLE.value,
                    **DataclassHookMixin.get_metadata(hooks),
                }
