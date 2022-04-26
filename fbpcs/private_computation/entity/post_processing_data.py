#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class PostProcessingData:
    """Stores metadata of post processing tier of private computation

    Public attributes:
        dataset_timestamp: timestamp of the input dataset for private computation. For daily recurring private run,
                           timestamp would be start of the day for which dataset is selected. This timestamp is
                           currently used only in Private Attribution and is retrieved by partner while selecting dataset.
    """

    # TODO : Add breakdown key to PostProcessingData.
    dataset_timestamp: int = 0

    def __str__(self) -> str:
        # pyre-ignore
        return self.to_json()
