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
        self.assertEqual(scrub_summary.name_to_num_subs["AWS access key id"], 1)
        self.assertEqual(scrub_summary.name_to_num_subs["AWS secret access key"], 1)
        self.assertEqual(scrub_summary.name_to_num_subs["Meta Graph API token"], 1)

    def test_email_scrub(self) -> None:
        # valid cases
        valid_email_address_1 = "test@test.com"
        valid_email_address_2 = "test.test@test.com"
        valid_email_address_3 = "test_test@test.com"

        # invalid cases
        invalid_email_address_1 = "test@"
        invalid_email_address_2 = "@test.com"
        invalid_email_address_3 = "test @test.com"

        test_message = f"""
        valid_email_address_1: {valid_email_address_1}
        valid_email_address_2: {valid_email_address_2}
        valid_email_address_3: {valid_email_address_3}

        invalid_email_address_1: {invalid_email_address_1}
        invalid_email_address_2: {invalid_email_address_2}
        invalid_email_address_3: {invalid_email_address_3}
        """

        expected_output = f"""
        valid_email_address_1: {self.scrubber.REPLACEMENT_STR}
        valid_email_address_2: {self.scrubber.REPLACEMENT_STR}
        valid_email_address_3: {self.scrubber.REPLACEMENT_STR}

        invalid_email_address_1: {invalid_email_address_1}
        invalid_email_address_2: {invalid_email_address_2}
        invalid_email_address_3: {invalid_email_address_3}
        """

        scrub_summary = self.scrubber.scrub(test_message)

        self.assertEqual(scrub_summary.scrubbed_output, expected_output)
        self.assertEqual(scrub_summary.total_substitutions, 3)
        self.assertEqual(scrub_summary.name_to_num_subs["Email address"], 3)

    def test_credit_Card_scrub(self) -> None:
        # valid cases
        valid_visa_credit_card_1 = "1234-1234-1234-1234"
        valid_visa_credit_card_2 = "1234123412341234"
        valid_mastercard_credit_card_1 = "1234-123456-12345"
        valid_mastercard_credit_card_2 = "123412345612345"

        # invalid cases
        invalid_visa_credit_card_1 = "123-1234-1234-1234"
        invalid_visa_credit_card_2 = "12341234123412342"
        invalid_visa_credit_card_3 = "123-1234- 1234-1234"
        invalid_mastercard_credit_card_1 = "1234-1234567-12345"
        invalid_mastercard_credit_card_2 = "12341234561234"
        invalid_mastercard_credit_card_3 = "1234- 1234567-12345"

        test_message = f"""
        valid_visa_credit_card_1: {valid_visa_credit_card_1}
        valid_visa_credit_card_2: {valid_visa_credit_card_2}
        valid_mastercard_credit_card_1: {valid_mastercard_credit_card_1}
        valid_mastercard_credit_card_2: {valid_mastercard_credit_card_2}

        invalid_visa_credit_card_1: {invalid_visa_credit_card_1}
        invalid_visa_credit_card_2: {invalid_visa_credit_card_2}
        invalid_visa_credit_card_3: {invalid_visa_credit_card_3}
        invalid_mastercard_credit_card_1: {invalid_mastercard_credit_card_1}
        invalid_mastercard_credit_card_2: {invalid_mastercard_credit_card_2}
        invalid_mastercard_credit_card_3: {invalid_mastercard_credit_card_3}
        """

        expected_output = f"""
        valid_visa_credit_card_1: {self.scrubber.REPLACEMENT_STR}
        valid_visa_credit_card_2: {self.scrubber.REPLACEMENT_STR}
        valid_mastercard_credit_card_1: {self.scrubber.REPLACEMENT_STR}
        valid_mastercard_credit_card_2: {self.scrubber.REPLACEMENT_STR}

        invalid_visa_credit_card_1: {invalid_visa_credit_card_1}
        invalid_visa_credit_card_2: {invalid_visa_credit_card_2}
        invalid_visa_credit_card_3: {invalid_visa_credit_card_3}
        invalid_mastercard_credit_card_1: {invalid_mastercard_credit_card_1}
        invalid_mastercard_credit_card_2: {invalid_mastercard_credit_card_2}
        invalid_mastercard_credit_card_3: {invalid_mastercard_credit_card_3}
        """

        scrub_summary = self.scrubber.scrub(test_message)

        self.assertEqual(scrub_summary.scrubbed_output, expected_output)
        self.assertEqual(scrub_summary.total_substitutions, 4)
        self.assertEqual(scrub_summary.name_to_num_subs["Credit card number"], 4)

    def test_date_of_birth_scrub(self) -> None:
        # valid case
        valid_dob_1 = "10-31-2021"
        valid_dob_2 = "31-10-2021"
        valid_dob_3 = "09/20/1947"
        valid_dob_4 = "20/09/2000"
        valid_dob_5 = "09 13 1900"
        valid_dob_6 = "20 09 2020"

        # invalid cases
        invalid_dob_1 = "10-32-2021"
        invalid_dob_2 = "31-13-2021"
        invalid_dob_3 = "09/20 /1947"
        invalid_dob_4 = "020/09/2000"
        invalid_dob_5 = "09 13 19003"
        invalid_dob_6 = "20 13 2020"

        test_message = f"""
        valid_dob_1: {valid_dob_1}
        valid_dob_2: {valid_dob_2}
        valid_dob_3: {valid_dob_3}
        valid_dob_4: {valid_dob_4}
        valid_dob_5: {valid_dob_5}
        valid_dob_6: {valid_dob_6}

        invalid_dob_1: {invalid_dob_1}
        invalid_dob_2: {invalid_dob_2}
        invalid_dob_3: {invalid_dob_3}
        invalid_dob_4: {invalid_dob_4}
        invalid_dob_5: {invalid_dob_5}
        invalid_dob_6: {invalid_dob_6}
        """

        expected_output = f"""
        valid_dob_1: {self.scrubber.REPLACEMENT_STR}
        valid_dob_2: {self.scrubber.REPLACEMENT_STR}
        valid_dob_3: {self.scrubber.REPLACEMENT_STR}
        valid_dob_4: {self.scrubber.REPLACEMENT_STR}
        valid_dob_5: {self.scrubber.REPLACEMENT_STR}
        valid_dob_6: {self.scrubber.REPLACEMENT_STR}

        invalid_dob_1: {invalid_dob_1}
        invalid_dob_2: {invalid_dob_2}
        invalid_dob_3: {invalid_dob_3}
        invalid_dob_4: {invalid_dob_4}
        invalid_dob_5: {invalid_dob_5}
        invalid_dob_6: {invalid_dob_6}
        """

        scrub_summary = self.scrubber.scrub(test_message)

        self.assertEqual(scrub_summary.scrubbed_output, expected_output)
        self.assertEqual(scrub_summary.total_substitutions, 6)
        self.assertEqual(scrub_summary.name_to_num_subs["Date of birth DDMMYY"], 3)
        self.assertEqual(scrub_summary.name_to_num_subs["Date of birth MMDDYY"], 3)

    def test_phone_number_scrub(self) -> None:
        # valid cases
        valid_phone_number_1 = "+19191231234"
        valid_phone_number_2 = "+1 (919) 123 1234"
        valid_phone_number_3 = "+1-(919)-123-1234"
        valid_phone_number_4 = "+123 919 123 1234"

        # invalid cases

        test_message = f"""
        valid_phone_number_1: {valid_phone_number_1}
        valid_phone_number_2: {valid_phone_number_2}
        valid_phone_number_3: {valid_phone_number_3}
        valid_phone_number_4: {valid_phone_number_4}
        """

        expected_output = f"""
        valid_phone_number_1: {self.scrubber.REPLACEMENT_STR}
        valid_phone_number_2: {self.scrubber.REPLACEMENT_STR}
        valid_phone_number_3: {self.scrubber.REPLACEMENT_STR}
        valid_phone_number_4: {self.scrubber.REPLACEMENT_STR}
        """

        scrub_summary = self.scrubber.scrub(test_message)

        self.assertEqual(scrub_summary.scrubbed_output, expected_output)
        self.assertEqual(scrub_summary.total_substitutions, 4)
        self.assertEqual(scrub_summary.name_to_num_subs["Phone number"], 4)
