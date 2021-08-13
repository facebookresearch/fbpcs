#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import patch

from fbpmp.scripts import gen_config


class TestGenConfig(unittest.TestCase):
    def test_prompt(self):

        # Test if Valid replacement exists and --accept_all passed - we use existing
        res = gen_config.prompt("key", replacements={"key": "baz"}, accept_all=True)
        self.assertEqual(res, "baz")

        # Test with an actual value provided
        with patch("builtins.input", return_value="foo"):
            # 1. No valid replacement
            res = gen_config.prompt("key", replacements={"bar": "baz"})
            self.assertEqual(res, "foo")

            # 2. Valid replacement exists and we override
            res = gen_config.prompt("key", replacements={"key": "baz"})
            self.assertEqual(res, "foo")

        # Test with hitting enter without typing
        with patch("builtins.input", return_value=""):
            # 1. No valid replacement
            res = gen_config.prompt("key", replacements={"bar": "baz"})
            self.assertEqual(res, "")

            # 2. Valid replacement exists and we keep
            res = gen_config.prompt("key", replacements={"key": "baz"})
            self.assertEqual(res, "baz")

    def test_build_replacements_from_config(self):
        config = {
            "a": "123",
            "b": ["1", "2", "3"],
            "c": {"d": "e"}
        }
        # This will look weird, but basically we expect to keep all "leaf"
        # nodes as replacement values, excluding lists
        expected = {
            "a": "123",
            "d": "e",
        }
        res = gen_config.build_replacements_from_config(config)
        self.assertEqual(res, expected)

    @patch("builtins.input", return_value="new")
    def test_update_dict(self, mock_input):
        # Simple replacement (call prompt 1 time)
        d = {"key": "REPLACE"}
        expected = {"key": "new"}
        gen_config.update_dict(d, replace_key="REPLACE")
        self.assertEqual(d, expected)
        self.assertEqual(mock_input.call_count, 1)

        # Replace within a nested dict (call prompt 2 times)
        d = {"key": "REPLACE", "key2": {"key3": "REPLACE"}}
        expected = {"key": "new", "key2": {"key3": "new"}}
        gen_config.update_dict(d, replace_key="REPLACE")
        self.assertEqual(d, expected)
        self.assertEqual(mock_input.call_count, 3)

        # Replace within existing replaement (no input called so mock_input.call_count does not change)
        d = {"key": "REPLACE"}
        replacements = {"key": "new"}
        expected = {"key": "new"}
        gen_config.update_dict(
            d, replace_key="REPLACE", replacements=replacements, accept_all=True
        )
        self.assertEqual(d, expected)
        self.assertEqual(mock_input.call_count, 3)

    @patch("fbpcs.util.yaml.load", return_value="LOAD")
    @patch("fbpcs.util.yaml.dump")
    @patch("fbpmp.scripts.gen_config.update_dict")
    def test_gen_config(self, mock_update, mock_dump, mock_load):
        args = {
            "<input_path>": "foo",
            "<new_output_path>": "bar",
            "--replace": "REPLACE",
            "--accept_all": True,
        }
        gen_config.gen_config(args)

        mock_load.assert_called_once_with("foo")
        mock_update.assert_called_once_with("LOAD", "REPLACE", {}, True)
        mock_dump.assert_called_once_with("LOAD", "bar")
