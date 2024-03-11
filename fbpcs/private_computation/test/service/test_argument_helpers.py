#!/usr/bin/env fbpython
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from unittest import TestCase

from fbpcs.private_computation.service.argument_helper import get_tls_arguments
from fbpcs.private_computation.service.constants import (
    CA_CERT_PATH,
    PRIVATE_KEY_PATH,
    SERVER_CERT_PATH,
)


class TestArgumentHelpers(TestCase):
    def test_get_tls_arguments_no_feature(self):
        tls_arguments = get_tls_arguments(False, "some path", "some path")

        self.assertFalse(tls_arguments["use_tls"])
        self.assertEqual(tls_arguments["ca_cert_path"], "")
        self.assertEqual(tls_arguments["server_cert_path"], "")
        self.assertEqual(tls_arguments["private_key_path"], "")

    def test_get_tls_arguments_with_feature(self):
        tls_arguments = get_tls_arguments(True, SERVER_CERT_PATH, CA_CERT_PATH)

        self.assertTrue(tls_arguments["use_tls"])
        self.assertEqual(tls_arguments["ca_cert_path"], CA_CERT_PATH)
        self.assertEqual(tls_arguments["server_cert_path"], SERVER_CERT_PATH)
        self.assertEqual(tls_arguments["private_key_path"], PRIVATE_KEY_PATH)
