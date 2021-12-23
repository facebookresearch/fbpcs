#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Dict

IS_FROZEN_FIELD_METADATA_STR: str = "instance_base_is_field_frozen"
IS_FROZEN_FIELD_DEFAULT_VALUE: bool = False


class InstanceBaseMetadata:
    MUTABLE: Dict[str, bool] = {IS_FROZEN_FIELD_METADATA_STR: False}
    IMMUTABLE: Dict[str, bool] = {IS_FROZEN_FIELD_METADATA_STR: True}
