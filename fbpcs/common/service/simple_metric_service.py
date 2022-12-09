#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json

from fbpcs.common.service.metric_service import MetricService


class SimpleMetricService(MetricService):
    def bump_entity_key(self, entity: str, key: str, value: int = 1) -> None:
        result = {
            "operation": "bump_entity_key",
            "entity": f"{self.category}.{entity}",
            "key": key,
            "value": value,
        }
        self.logger.debug(json.dumps(result))

    def bump_entity_key_avg(self, entity: str, key: str, value: int) -> None:
        result = {
            "operation": "bump_entity_key_avg",
            "entity": f"{self.category}.{entity}",
            "key": key,
            "value": value,
        }
        self.logger.debug(json.dumps(result))
