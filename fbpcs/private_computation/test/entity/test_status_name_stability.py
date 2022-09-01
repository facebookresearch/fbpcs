#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import pathlib
import unittest

from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.test.entity.gen_status_resources import (
    OUTPUT_DIR,
    STATUS_NAME_SUFFIX,
)


class TestStatusNameStability(unittest.TestCase):
    def _check_status_names(self, path: pathlib.Path) -> None:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line.startswith("#") and len(line) > 0:
                    status_name = line
                    try:
                        PrivateComputationInstanceStatus(status_name)
                    except ValueError:
                        self.fail(f"Failed to find status name {status_name}")

    def test_status_name_stability(self) -> None:
        for fn in os.listdir(OUTPUT_DIR):
            if os.path.isfile(OUTPUT_DIR / fn) and fn.endswith(STATUS_NAME_SUFFIX):
                with self.subTest(fn):
                    self._check_status_names(OUTPUT_DIR / fn)
