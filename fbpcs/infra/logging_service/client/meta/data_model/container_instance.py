# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass

from dataclasses_json import dataclass_json
from fbpcs.infra.logging_service.client.meta.data_model.log_location import LogLocation

# Log metadata for a study run
@dataclass_json
@dataclass
class ContainerInstance:
    container_id: str
    start_ts: str
    log_location: LogLocation
