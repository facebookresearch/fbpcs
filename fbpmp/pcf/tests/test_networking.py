#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import socket
import unittest

import fbpmp.pcf.networking as networking


class TestNetworking(unittest.TestCase):
    def test_find_free_port(self):
        port = networking.find_free_port()
        # Refer to this to check if port is taken:
        # https://stackoverflow.com/questions/2470971/fast-way-to-test-if-a-port-is-in-use-using-python
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self.assertNotEqual(s.connect_ex(("", port)), 0)
