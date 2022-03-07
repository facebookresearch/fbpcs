# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from collections import Counter
from typing import Any, Dict


class ValidationIssues:
    def __init__(self) -> None:
        self.empty_counter: Counter[str] = Counter()

    def get_as_dict(self) -> Dict[str, Any]:
        result = {}
        empty_id_count = self.empty_counter["id_"]
        empty_value_count = self.empty_counter["value"]
        empty_event_timestamp_count = self.empty_counter["event_timestamp"]
        if empty_id_count > 0:
            result["id_"] = {"empty": empty_id_count}
        if empty_value_count > 0:
            result["value"] = {"empty": empty_value_count}
        if empty_event_timestamp_count > 0:
            result["event_timestamp"] = {"empty": empty_event_timestamp_count}

        return result

    def count_empty_field(self, field: str) -> None:
        self.empty_counter[field] += 1
