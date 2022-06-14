# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from typing import Any, Dict, List, Type, Union

from fbpcs.infra.pce_deployment_library.cloud_library.aws.aws import AWS
from fbpcs.infra.pce_deployment_library.cloud_library.cloud_base.cloud_base import (
    CloudBase,
)
from fbpcs.infra.pce_deployment_library.cloud_library.defaults import CloudPlatforms
from fbpcs.infra.pce_deployment_library.cloud_library.gcp.gcp import GCP


class CloudFactory:
    CLOUD_TYPES: Dict[CloudPlatforms, Type[Union[AWS, GCP]]] = {
        CloudPlatforms.AWS: AWS,
        CloudPlatforms.GCP: GCP,
    }

    def create_cloud_object(
        self, cloud_type: CloudPlatforms, **kwargs: Any
    ) -> CloudBase:
        supported_cloud_platform = self.get_supported_cloud_platforms()
        if self.CLOUD_TYPES.get(cloud_type, None) is None:
            raise Exception(
                f"{cloud_type} is not a supported cloud platform. Supported platforms are {supported_cloud_platform}"
            )
        return self.CLOUD_TYPES[cloud_type](**kwargs)

    def get_supported_cloud_platforms(self) -> List[str]:
        return CloudPlatforms.list()
