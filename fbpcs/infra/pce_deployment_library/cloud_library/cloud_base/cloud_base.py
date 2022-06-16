# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import ABC, abstractmethod

from fbpcs.infra.pce_deployment_library.cloud_library.defaults import CloudPlatforms


class CloudBase(ABC):
    @classmethod
    @abstractmethod
    def cloud_type(cls) -> CloudPlatforms:
        pass
