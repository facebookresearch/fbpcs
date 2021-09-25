#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


class PrivateComputationServiceValidationError(ValueError):
    """
    Error raised when private_computation_service.validate_metrics found the aggregated results doesn't match the expected results.
    """
