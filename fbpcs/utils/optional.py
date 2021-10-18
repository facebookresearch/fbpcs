#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import TypeVar, Optional

T = TypeVar("T")


def unwrap_or_default(optional: Optional[T], default: T) -> T:
    if optional is None:
        return default
    return optional
