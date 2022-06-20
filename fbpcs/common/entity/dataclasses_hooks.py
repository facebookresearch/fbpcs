# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from abc import abstractmethod
from dataclasses import dataclass
from enum import auto, Enum
from typing import Any, ClassVar, Dict, Generic, Iterable, List, Optional, TypeVar


class HookEventType(Enum):
    PRE_INIT = auto()
    POST_INIT = auto()
    PRE_UPDATE = auto()
    POST_UPDATE = auto()
    PRE_DELETE = auto()
    POST_DELETE = auto()


T = TypeVar("T")


class DataclassHook(Generic[T]):
    @abstractmethod
    def run(
        self,
        instance: T,
        field_name: str,
        previous_field_value: Any,
        new_field_value: Any,
        hook_event: HookEventType,
    ) -> None:
        ...

    @property
    @abstractmethod
    def triggers(self) -> Iterable[HookEventType]:
        ...


@dataclass
class DataclassHookMixin:
    HOOK_METADATA_STR: ClassVar[str] = "DataclassHook_metadata_str"

    def __setattr__(self, name: str, value: Any) -> None:
        old_value = getattr(self, name, None)
        # if the field was previously defined
        if old_value:
            # If so, run the PRE_UPDATE hooks, set the field, then run the POST_UPDATE hooks
            self._run_hooks(HookEventType.PRE_UPDATE, name, old_value, value)
            super().__setattr__(name, value)
            self._run_hooks(HookEventType.POST_UPDATE, name, old_value, value)
        else:
            # If not, run the PRE_INIT hooks, set the field, then run the POST_INIT hooks
            self._run_hooks(HookEventType.PRE_INIT, name, None, value)
            # use super here to avoid calling self.setter, which causes stack overflow
            super().__setattr__(name, value)
            self._run_hooks(HookEventType.POST_INIT, name, None, value)

    def __delattr__(self, name: str) -> None:
        """
        In dataclass,
        an attribute with default value (default_factory not include) can never be truly deleted.
        After deleting an attribute with default value (even if you might have updated it already)
        you can still get access to its default value.
        """
        # Run the PRE_DELETE hooks, delete the field, then run the POST_DELETE hooks
        old_value = getattr(self, name, None)
        self._run_hooks(HookEventType.PRE_DELETE, name, old_value)
        del self.__dict__[name]
        self._run_hooks(HookEventType.POST_DELETE, name, old_value)

    def _get_hooks(
        self,
        hook_type: HookEventType,
        field_name: str,
    ) -> Iterable[DataclassHook]:
        hooks: List[DataclassHook] = []
        # pyre-fixme Undefined attribute [16]: DataclassHookMixin has no attribute __dataclass_fields__
        hook_pool: Iterable[DataclassHook] = self.__dataclass_fields__[
            field_name
        ].metadata.get(DataclassHookMixin.HOOK_METADATA_STR)
        if hook_pool is not None:
            for h in hook_pool:
                triggers: Iterable[HookEventType] = h.triggers
                if hook_type in triggers:
                    hooks.append(h)
        return hooks

    def _run_hooks(
        self,
        hook_type: HookEventType,
        field_name: str,
        previous_field_value: Optional[Any] = None,
        new_field_value: Optional[Any] = None,
    ) -> None:
        hooks: Iterable[DataclassHook] = self._get_hooks(hook_type, field_name)
        for hook in hooks:
            hook.run(self, field_name, previous_field_value, new_field_value, hook_type)

    @staticmethod
    def get_metadata(
        *args: DataclassHook,
    ) -> Dict[str, Iterable[DataclassHook]]:
        return {DataclassHookMixin.HOOK_METADATA_STR: args}
