# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class InfraConfig:
    """Stores metadata of infra config in a private computation instance

    Public attributes:

    Private attributes:

    """
