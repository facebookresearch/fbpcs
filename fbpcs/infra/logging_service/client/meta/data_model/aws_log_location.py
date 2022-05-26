# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass

from dataclasses_json import dataclass_json


# Log metadata for a study run
@dataclass_json
@dataclass
class AwsLogLocation:
    log_group: str
    log_stream: str
