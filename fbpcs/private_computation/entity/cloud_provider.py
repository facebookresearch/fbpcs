#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from enum import Enum

from measurement.private_measurement.pcp.pce.pce_service.pce_service_thrift_base.types import (
    CloudProvider as CloudProviderThriftType,
)


class CloudProvider(Enum):
    AWS = 1
    GCP = 2

    @staticmethod
    def from_thrift(
        provider_type: CloudProviderThriftType,
    ) -> "CloudProvider":
        return {
            CloudProviderThriftType.AWS: CloudProvider.AWS,
            CloudProviderThriftType.GCP: CloudProvider.GCP,
        }[provider_type]
