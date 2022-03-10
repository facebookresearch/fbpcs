#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class PIDStageInput:
    input_paths: List[str]
    output_paths: List[str]
    num_shards: int
    instance_id: str
    is_validating: Optional[bool] = False
    synthetic_shard_path: Optional[str] = None
    hmac_key: Optional[str] = None
    pid_use_row_numbers: bool = False

    def add_to_inputs(self, input_path: str) -> None:
        self.input_paths.append(input_path)
