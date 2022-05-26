# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass

from dataclasses_json import dataclass_json

# Base data model of log metadata
@dataclass_json
@dataclass
class BaseInfo:
    # Version contains date and optional build number. E.g. "2022-04-11", "2022-04-11.126".
    version: str
    # Type of the info data
    info_type: str
