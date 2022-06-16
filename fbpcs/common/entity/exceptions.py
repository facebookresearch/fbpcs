# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Any


class InstanceBaseError(Exception):
    pass


class InstanceFrozenFieldError(RuntimeError, InstanceBaseError):
    def __init__(self, name: str) -> None:
        msg = (
            f"Cannot change value of {name} because it is marked as an immutable field."
        )
        super().__init__(msg)


class HookError(Exception):
    pass


class OutOfRangeHookError(RuntimeError, HookError):
    def __init__(
        self, name: str, value: Any, min_value: float, max_value: float
    ) -> None:
        msg = f"Cannot change {name} with value: {value} because it is beyond range of [{min_value},  {max_value}]"
        super().__init__(msg)


class MissingRangeHookError(RuntimeError, HookError):
    def __init__(self, name: str) -> None:
        msg = f"Cannot create a range hook of {name} because min and max value are both missing."
        super().__init__(msg)
