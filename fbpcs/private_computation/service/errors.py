#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


class PrivateComputationServiceBaseException(Exception):
    pass


class PrivateComputationServiceValidationError(
    PrivateComputationServiceBaseException, ValueError
):
    """
    Error raised when private_computation_service.validate_metrics found the aggregated results doesn't match the expected results.
    """


class PrivateComputationServiceInvalidStageError(
    PrivateComputationServiceBaseException, RuntimeError
):
    pass
