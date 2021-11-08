# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Dict, Set
import re

ALL_REQUIRED_FIELDS: Set[str] = {
    'action_source',
    'conversion_value',
    'currency_type',
    'event_type',
    'timestamp',
}
ONE_OR_MORE_REQUIRED_FIELDS: Set[str] = {'email','device_id'}
FORMAT_VALIDATION_FOR_FIELD: Dict[str, re.Pattern] = {
    'email': re.compile(r"^[a-f0-9]{64}$"),
    'device_id': re.compile(r"^([a-f0-9]{32}|[a-f0-9-]{36})$"),
    'timestamp': re.compile(r"^[0-9]+$"),
    'currency_type': re.compile(r"^[a-z]+$"),
    'conversion_value': re.compile(r"^[0-9]+$"),
    'action_source': re.compile(r"^(email|website|phone_call|chat|physical_store|system_generated|other)$"),
    'event_type': re.compile(r"^.+$"),
}
