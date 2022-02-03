#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre strict

import json
from typing import Any

from fbpcs.pid.service.coordination.coordination import CoordinationService


class FileCoordinationService(CoordinationService):
    def _is_coordination_object_ready(self, value: str) -> bool:
        storage_svc = self.storage_svc
        if storage_svc is None:
            raise ValueError("self.storage_svc is None")
        return storage_svc.file_exists(value)

    def _put_data(self, value: str, data: Any) -> None:
        """
        Default behavior is to simply JSON serialize the data
        """
        storage_svc = self.storage_svc
        if storage_svc is None:
            raise ValueError("self.storage_svc is None")
        payload = json.dumps(data)
        storage_svc.write(value, payload)

    def _get_data(self, value: str) -> Any:
        storage_svc = self.storage_svc
        if storage_svc is None:
            raise ValueError("self.storage_svc is None")
        payload = storage_svc.read(value)
        return json.loads(payload)
