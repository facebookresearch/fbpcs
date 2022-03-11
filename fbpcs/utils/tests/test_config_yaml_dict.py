#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json
import unittest
from unittest.mock import patch, mock_open

from fbpcs.utils.config_yaml.config_yaml_dict import ConfigYamlDict
from fbpcs.utils.config_yaml.exceptions import ConfigYamlFileParsingError


class TestConfigYamlDict(unittest.TestCase):
    test_filename = "./config.yaml"
    test_dict = {
        "test_dict": [
            {"test_key_1": "test_value_1"},
            {"test_key_1": "test_value_2"},
        ]
    }
    valid_data = json.dumps(test_dict)
    invalid_data = """
    test_dict:
        test_key_1: test_value_1
        test_key_2
    """

    @patch("builtins.open", new_callable=mock_open, read_data=valid_data)
    def test_load_from_file_success(self, mock_file) -> None:
        self.assertEqual(open(self.test_filename).read(), self.valid_data)

        load_data = ConfigYamlDict.from_file(self.test_filename)
        self.assertEqual(load_data, self.test_dict)

    @patch("builtins.open", new_callable=mock_open, read_data=invalid_data)
    def test_load_from_invalid_file(self, mock_file) -> None:
        self.assertEqual(open(self.test_filename).read(), self.invalid_data)

        with self.assertRaises(ConfigYamlFileParsingError) as error_context:
            ConfigYamlDict.from_file(self.test_filename)
            self.assertTrue(
                str(error_context.exception).startswith(
                    f"""
                    {self.test_filename} is not a valid YAML file.
                    Please make sure that the content of your config is a valid YAML.
                    \nCause:"""
                )
            )
