#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import TestCase

from fbpcs.common.service.secret_scrubber import SecretScrubber


class TestSecretScrubber(TestCase):
    def setUp(self) -> None:
        self.scrubber = SecretScrubber()
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

    def test_scrub(self) -> None:
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
        for c in scrub_summary.name_to_num_subs.values():
            self.assertEqual(c, 1)

    def test_pii_scrubber_regex(self) -> None:
        # Adding Logging Service PII scubber related regex
        # These regex are not expected to be scrubbed
        sha256 = "746df05c8f3a0b07a46c0967cfbc5cbe5b9d48d0f79b6177eeedf8be6c8b34b5"
        email = "test@test.com"
        test_message = f"""
        sha256: "{sha256}"
        email: "{email}"
        """

        expected_output = f"""
        sha256: "{sha256}"
        email: "{email}"
        """

        scrub_summary = self.scrubber.scrub(test_message)

        self.assertEqual(scrub_summary.scrubbed_output, expected_output)
        self.assertEqual(scrub_summary.total_substitutions, 0)
        for c in scrub_summary.name_to_num_subs.values():
            self.assertEqual(c, 0)
