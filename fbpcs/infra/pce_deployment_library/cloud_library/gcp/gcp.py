# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from fbpcs.infra.pce_deployment_library.cloud_library.cloud_base.cloud_base import (
    CloudBase,
)
from fbpcs.infra.pce_deployment_library.cloud_library.defaults import CloudPlatforms


class GCP(CloudBase):
    @classmethod
    def cloud_type(cls) -> CloudPlatforms:
        return CloudPlatforms.GCP
