#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import TestCase

from fbpcs.common.service.pii_scrubber import PiiLoggingScrubber


class TestPiiScrubber(TestCase):
    def setUp(self) -> None:
        self.scrubber = PiiLoggingScrubber()
        # these are not real. I randomly generated them
        # and separated them into two string to avoid any
        # scanners picking them up
        # used exrex: https://github.com/asciimoo/exrex
        self.aws_access_key_id = "L194ZYK14" + "K8XMRS9RIEE"
        self.aws_secret_access_key = "NJuU/e4plYJ" + "gc6ykqaykh6yXoQxYFmdO+RNJtYHV"
        self.graph_api_token = (
            "EAA3XZGPxS4159hjkreOqw0cUmSLx6K6fzZvemMGnzSQrjLadM"
            "uhMS1ZeobDYIv4kBzGC5hU066Zj5ud0xNqqeNv3OhVJvYzqXUKit6Lj"
        )

    def test_secret_scrub_patterns(self) -> None:
        test_message = f"""
        "CloudCredentialService": {{
          "class": "fbpcs.pid.service.credential_service.simple_cloud_credential_service.SimpleCloudCredentialService",
          "constructor": {{
            "access_key_id": "{self.aws_access_key_id}",
            "access_key_data": "{self.aws_secret_access_key}"
          }}
        }}

        access_token: {self.graph_api_token}
        """

        expected_output = f"""
        "CloudCredentialService": {{
          "class": "fbpcs.pid.service.credential_service.simple_cloud_credential_service.SimpleCloudCredentialService",
          "constructor": {{
            "access_key_id": "{self.scrubber.REPLACEMENT_STR}",
            "access_key_data": "{self.scrubber.REPLACEMENT_STR}"
          }}
        }}

        access_token:{self.scrubber.REPLACEMENT_STR}
        """

        scrub_summary = self.scrubber.scrub(test_message)

        self.assertEqual(scrub_summary.scrubbed_output, expected_output)
        self.assertEqual(scrub_summary.total_substitutions, 3)
        self.assertEqual(scrub_summary.name_to_num_subs["AWS access key id"], 1)
        self.assertEqual(scrub_summary.name_to_num_subs["AWS secret access key"], 1)
        self.assertEqual(scrub_summary.name_to_num_subs["Meta Graph API token"], 1)

    def test_athena_base_hash(self) -> None:
        valid_hash_key1 = "FROM_BASE64('lLzSY6e+ATeKX11bHRefw0omuDh5HnUNfBu4tt6tg8o=')"
        valid_hash_key2 = "FROM_BASE64('1234')"
        valid_hash_key3 = "FROM_BASE64('abcDABCd')"

        invalid_hash_key1 = (
            "FROM_BASE32('lLzSY6e+ATeKX11bHRefw0omuDh5HnUNfBu4tt6tg8o=')"
        )
        invalid_hash_key2 = "lLzSY6e+ATeKX11bHRefw0omuDh5HnUNfBu4tt6tg8o="
        invalid_hash_key3 = "1234"
        invalid_hash_key4 = "abcDABCd"

        test_message = f"""
        valid_hash_key1 = {valid_hash_key1}
        valid_hash_key2 = {valid_hash_key2}
        valid_hash_key3 = {valid_hash_key3}

        invalid_hash_key1 = {invalid_hash_key1}
        invalid_hash_key2 = {invalid_hash_key2}
        invalid_hash_key3 = {invalid_hash_key3}
        invalid_hash_key4 = {invalid_hash_key4}
        """

        expected_output = f"""
        valid_hash_key1 = FROM_BASE64('{self.scrubber.REPLACEMENT_STR})
        valid_hash_key2 = FROM_BASE64('{self.scrubber.REPLACEMENT_STR})
        valid_hash_key3 = FROM_BASE64('{self.scrubber.REPLACEMENT_STR})

        invalid_hash_key1 = {invalid_hash_key1}
        invalid_hash_key2 = {invalid_hash_key2}
        invalid_hash_key3 = {invalid_hash_key3}
        invalid_hash_key4 = {invalid_hash_key4}
        """

        scrub_summary = self.scrubber.scrub(test_message)

        self.assertEqual(scrub_summary.scrubbed_output, expected_output)
        self.assertEqual(scrub_summary.total_substitutions, 3)
        self.assertEqual(scrub_summary.name_to_num_subs["Athena Hash Key"], 3)
        self.assertEqual(scrub_summary.name_to_num_subs["AWS access key id"], 0)
        self.assertEqual(scrub_summary.name_to_num_subs["AWS secret access key"], 0)
        self.assertEqual(scrub_summary.name_to_num_subs["Meta Graph API token"], 0)
