# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from enum import Enum


IS_FROZEN_FIELD: str = "mutability"


class InstanceBaseMetadata(Enum):
    MUTABLE = {IS_FROZEN_FIELD: False}
    IMMUTABLE = {IS_FROZEN_FIELD: True}
