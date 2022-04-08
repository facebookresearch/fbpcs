# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from dataclasses import dataclass
from typing import Optional


# Data model of log metadata
@dataclass
class MetadataEntity:
    partner_id: str
    entity_key: str
    entity_value: Optional[str] = None
