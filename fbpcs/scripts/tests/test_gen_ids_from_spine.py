#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import call, mock_open, patch

from fbpcs.scripts import gen_ids_from_spine


NUM_IDS = 100


class TestGenIdsFromSpine(unittest.TestCase):
    def test_gen_ids_from_spine(self):
        # NUM_IDS integers with a trailing newline at the end of the file
        input_lines = "\n".join(str(i) for i in range(NUM_IDS)) + "\n"

        args = {
            "<spine_path>": "mock_read",
            "<output_path>": "mock_write",
            "--keep_rate": 1.0,
            "--log_every_n": None,
        }

        # First test with a 100% keep rate
        m = mock_open(read_data=input_lines)
        with patch("builtins.open", m):
            gen_ids_from_spine.gen_ids_from_spine(args)

        m.assert_has_calls(
            [call(args["<spine_path>"]), call(args["<output_path>"], "w")],
            any_order=True,
        )
        handle = m()
        calls = [call(f"{i}\n") for i in range(NUM_IDS)]
        handle.write.assert_has_calls(calls, any_order=True)

        # Then test with a 0% keep rate
        args["--keep_rate"] = 0.0
        m = mock_open(read_data=input_lines)
        with patch("builtins.open", m):
            gen_ids_from_spine.gen_ids_from_spine(args)
        m.assert_has_calls(
            [call(args["<spine_path>"]), call(args["<output_path>"], "w")],
            any_order=True,
        )
        handle = m()
        calls = [call(f"{i}\n") for i in range(NUM_IDS)]
        for c in calls:
            self.assertNotIn(c, handle.write.mock_calls)
