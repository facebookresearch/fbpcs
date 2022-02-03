#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import json


def is_json_equal(json_path_a: str, json_path_b: str) -> bool:
    with open(json_path_a) as json_file_a, open(json_path_b) as json_file_b:
        json_a = json.load(json_file_a)
        json_b = json.load(json_file_b)
        return json_a == json_b
