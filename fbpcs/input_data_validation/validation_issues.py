# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from typing import Any, Dict


class ValidationIssues:
    def __init__(self) -> None:
        self.empty_id = 0
        self.empty_value = 0
        self.empty_event_timestamp = 0

    def get_as_dict(self) -> Dict[str, Any]:
        result = {}
        if self.empty_id > 0:
            result["id_"] = {"empty": self.empty_id}
        if self.empty_value > 0:
            result["value"] = {"empty": self.empty_value}
        if self.empty_event_timestamp > 0:
            result["event_timestamp"] = {"empty": self.empty_event_timestamp}

        return result

    def count_empty_field(self, field: str) -> None:
        if field == "id_":
            self.empty_id += 1
        elif field == "value":
            self.empty_value += 1
        elif field == "event_timestamp":
            self.empty_event_timestamp += 1
