#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum

class PrivateComputationInstanceStatus(Enum):
    UNKNOWN = "UNKNOWN"
    CREATION_STARTED = "CREATION_STARTED"
    CREATED = "CREATED"
    CREATION_FAILED = "CREATION_FAILED"
    ID_MATCHING_STARTED = "ID_MATCHING_STARTED"
    ID_MATCHING_COMPLETED = "ID_MATCHING_COMPLETED"
    ID_MATCHING_FAILED = "ID_MATCHING_FAILED"
    COMPUTATION_STARTED = "COMPUTATION_STARTED"
    COMPUTATION_COMPLETED = "COMPUTATION_COMPLETED"
    COMPUTATION_FAILED = "COMPUTATION_FAILED"
    AGGREGATION_STARTED = "AGGREGATION_STARTED"
    AGGREGATION_COMPLETED = "AGGREGATION_COMPLETED"
    AGGREGATION_FAILED = "AGGREGATION_FAILED"
    POST_PROCESSING_HANDLERS_STARTED = "POST_PROCESSING_HANDLERS_STARTED"
    POST_PROCESSING_HANDLERS_COMPLETED = "POST_PROCESSING_HANDLERS_COMPLETED"
    POST_PROCESSING_HANDLERS_FAILED = "POST_PROCESSING_HANDLERS_FAILED"
    PROCESSING_REQUEST = "PROCESSING_REQUEST"
