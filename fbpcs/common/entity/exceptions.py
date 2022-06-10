# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


class InstanceBaseError(Exception):
    pass


class InstanceFrozenFieldError(RuntimeError, InstanceBaseError):
    def __init__(self, name: str) -> None:
        msg = (
            f"Cannot change value of {name} because it is marked as an immutable field."
        )
        super().__init__(msg)
