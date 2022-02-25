#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


class InstanceBaseError(Exception):
    pass


class InstanceDeserializationError(InstanceBaseError):
    pass


class InstanceVersionMismatchError(ValueError, InstanceDeserializationError):
    pass


class InstanceFrozenFieldError(RuntimeError, InstanceBaseError):
    pass
