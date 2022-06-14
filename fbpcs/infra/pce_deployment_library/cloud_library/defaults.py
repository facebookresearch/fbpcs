# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from enum import Enum
from typing import List


class CloudPlatforms(str, Enum):
    AWS = "aws"
    GCP = "gcp"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in CloudPlatforms]
