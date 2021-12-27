# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import re
from typing import List
from unittest import TestCase
from unittest.mock import Mock

from expected_fields import UNFILTERED_ONE_OR_MORE_REQUIRED_FIELDS
from validation import generate_from_body


class TestValidation(TestCase):
    def test_validate_requires_header_row(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(["bad,header,row", "1,2,3"])
        result = generate_from_body(body)
        self.assertRegex(result, "ERROR - The header row is not valid.")
        self.assertRegex(result, "1 or more of the required fields is missing.")
        self.assertRegex(result, "Validation processing stopped.")

    def test_validate_returns_number_of_rows(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id,browser_name,device_os,device_os_version,data_source_id\n",
                "1631204619,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456\n",
                "1631204619,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,abcd:1001::,Mozilla,fb.1.1558571054389.1098115397,123456,Mozella,Mac OS X,10.13.6,123456\n",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 2")
        self.assertRegex(result, "Valid rows: 2")

    def test_validate_private_attribution_data(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "id_,conversion_timestamp,conversion_value,conversion_metadata\n",
                "abcd/1234+WXYZ=,1631204619,2000,0",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 1")
        self.assertRegex(result, "Valid rows: 1")

    def test_validate_private_lift_data(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "id_,event_timestamp,value\n",
                "abcd/1234+WXYZ=,1631204619,2000\n",
                "abcd/1234+WXYZ=,1631204619,2000\n",
                "abcd/1234+WXYZ=,1631204619,2000\n",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 3")
        self.assertRegex(result, "Valid rows: 3")

    def test_validate_private_attribution_data_with_errors(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "id_,conversion_timestamp,conversion_value,conversion_metadata",
                "abcd/1234+WXYZ=,january,2000,0",
                "abcd/1234+WXYZ=,1631204619,fifty,0",
                "$@&**#$^$^,1631204619,2000,0",
                "abcd/1234+WXYZ===,1631204619,2000,0",
                "abcd/1234+WXYZ=,july-01-2021,2000,0",
                "abcd/1234+WXYZ,1631204619,2000,0",
                "abcd/1234+WXYZ=,1631204619,2000,test",
                "abcd/1234+WXYZ=,1631204619,123.99,0",
                ",1631204619,2000,0",
                "abcd/1234+WXYZ=,,2000,0",
                "abcd/1234+WXYZ=,1631204619,,0",
                "abcd/1234+WXYZ=,1631204619,2000,",
                "abcd/1234+WXYZ=,16312046190,2000,0",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 13")
        self.assertRegex(result, "Rows with errors: 12")
        self.assertRegex(result, "Valid rows: 1")
        self.assertRegex(result, "Line numbers with incorrect 'id_' format: 4,5")
        self.assertRegex(
            result, "Line numbers with incorrect 'conversion_timestamp' format: 2,6,14"
        )
        self.assertRegex(
            result, "Line numbers with incorrect 'conversion_value' format: 3,9"
        )
        self.assertRegex(
            result, "Line numbers with incorrect 'conversion_metadata' format: 8"
        )
        self.assertRegex(result, "Line numbers missing 'id_': 10")
        self.assertRegex(result, "Line numbers missing 'conversion_timestamp': 11")
        self.assertRegex(result, "Line numbers missing 'conversion_value': 12")
        self.assertRegex(result, "Line numbers missing 'conversion_metadata': 13")

    def test_validate_private_lift_data_with_errors(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "id_,event_timestamp,value",
                "abcd/1234+WXYZ=,1631204619,",
                "abcd/1234+WXYZ=,,2000",
                ",1631204619,2000",
                "abcd/1234+WXYZ=,1631204619,two",
                "abcd/1234+WXYZ=,test,2000",
                "abcd   WXYZ=,1631204619,2000",
                ".@/?-`!,1631204619,2000",
                "abcd/1234+WXYZ==,1631204619,2000",
                "abcd/1234+WXYZ=,1631204619,2000",
                "abcd/1234+WXYZ=,16312046190,2000",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 10")
        self.assertRegex(result, "Rows with errors: 8")
        self.assertRegex(result, "Valid rows: 2")
        self.assertRegex(result, "Line numbers with incorrect 'id_' format: 7,8")
        self.assertRegex(
            result, "Line numbers with incorrect 'event_timestamp' format: 6,11"
        )
        self.assertRegex(result, "Line numbers with incorrect 'value' format: 5")
        self.assertRegex(result, "Line numbers missing 'id_': 4")
        self.assertRegex(result, "Line numbers missing 'event_timestamp': 3")
        self.assertRegex(result, "Line numbers missing 'value': 2")

    def test_validate_returns_validation_counts(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id\n",
                ",,,,,,,,,,,,,\n",
                "1631204619,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456\n",
                ",,,,,,,,,,,,,\n",
                ",,,,,,,,,,,,,\n",
                "1631204619,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456\n",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 5")
        self.assertRegex(result, "Valid rows: 2")
        self.assertRegex(result, "Rows with errors: 3")

    def test_validate_fails_when_one_required_field_is_empty(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id,browser_name,device_os,device_os_version,data_source_id\n",
                ",usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,19191234567,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123,Chrome,Mac OS X,10.13.6,aaaaabbbbccccc\n",
                "1631204619,,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,19191234567,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123,Chrome,Mac OS X,10.13.6,aaaaabbbbccccc\n",
                "1631204619,usd,,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,19191234567,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123,Chrome,Mac OS X,10.13.6,aaaaabbbbccccc\n",
                "1631204619,usd,5,,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,19191234567,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123,Chrome,Mac OS X,10.13.6,aaaaabbbbccccc\n",
                "1631204619,usd,5,Purchase,,website,,2021,09,09,16,19191234567,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123,Chrome,Mac OS X,10.13.6,aaaaabbbbccccc\n",
                "1631204619,usd,5,,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,19191234567,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123,Chrome,Mac OS X,10.13.6,aaaaabbbbccccc",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 6")
        self.assertRegex(result, "Rows with errors: 6")
        self.assertRegex(result, "Valid rows: 0")

    def test_validate_handles_quoted_csvs(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                '"timestamp","currency_type","conversion_value","event_type","email","action_source","year","month","day","hour","phone","client_ip_address","client_user_agent","click_id","login_id"',
                '"","","","","","","","","","","","","","","",',
                '"1631204619","usd","5","Purchase","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111","website","2021","09","09","16","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111","10.0.0.1","Mozilla","fb.1.1558571054389.1098115397","123456"',
                '"","","","","","","","","","","","","","","",',
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 3")
        self.assertRegex(result, "Valid rows: 1")
        self.assertRegex(result, "Rows with errors: 2")

    def test_validate_reports_which_rows_are_missing_which_field(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id",
                "1631204621,,,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456",
                ",usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456",
                "1631204621,,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 4")
        self.assertRegex(result, "Rows with errors: 3")
        self.assertRegex(result, "Valid rows: 1")
        self.assertRegex(result, "Line numbers missing 'timestamp': 4")
        self.assertRegex(result, "Line numbers missing 'currency_type': 2,5")
        self.assertRegex(result, "Line numbers missing 'conversion_value': 2")
        self.assertRegex(result, "Line numbers missing 'action_source': 4,5")

    def test_validate_reports_which_rows_are_missing_all_identity_fields(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456",
                "1631204621,usd,5,Purchase,,website,,2021,09,09,16,,,,,",
                "1631204621,usd,5,Purchase,,website,,2021,09,09,16,,,,,",
                "1631204621,usd,5,Purchase,,website,,2021,09,09,16,,,,,",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456",
            ]
        )
        result = generate_from_body(body)
        expected_required_fields = ",".join(
            sorted(UNFILTERED_ONE_OR_MORE_REQUIRED_FIELDS)
        )
        self.assertRegex(result, "Total rows: 5")
        self.assertRegex(result, "Rows with errors: 3")
        self.assertRegex(result, "Valid rows: 2")
        self.assertRegex(
            result,
            f"Line numbers that are missing 1 or more of these required fields '{expected_required_fields}': 3,4,5",
        )

    def test_validate_report_lists_only_the_first_100_missing_lines_per_error(self):
        body = Mock("body")
        lines = [
            "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id",
            "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456",
        ]
        lines.extend(
            [
                ",usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456"
            ]
            * 100
        )
        lines.extend(
            [
                "1631204621,usd,5,,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456"
            ]
            * 101
        )
        lines.extend(["1631204621,usd,5,Purchase,,website,,2021,09,09,16,,,,,"] * 200)
        body.iter_lines = self.mock_lines_helper(lines)
        result = generate_from_body(body)
        expected_lines_missing_timestamp = ",".join(map(str, range(3, 103)))
        expected_lines_missing_event_type = ",".join(map(str, range(103, 203)))
        expected_fields_all_missing = ",".join(
            sorted(UNFILTERED_ONE_OR_MORE_REQUIRED_FIELDS)
        )
        expected_warning = r"\(First 100 lines shown\)"
        expected_lines_all_missing = ",".join(map(str, range(204, 304)))
        self.assertRegex(result, "Total rows: 402")
        self.assertRegex(result, "Rows with errors: 401")
        self.assertRegex(result, "Valid rows: 1")
        self.assertRegex(
            result,
            f"Line numbers missing 'timestamp': {expected_lines_missing_timestamp}",
        )
        self.assertRegex(
            result,
            f"Line numbers missing 'event_type' {expected_warning}: {expected_lines_missing_event_type}",
        )
        self.assertRegex(
            result,
            re.compile(
                f"Line numbers that are missing 1 or more of these required fields '{expected_fields_all_missing}' {expected_warning}: {expected_lines_all_missing}"
            ),
        )

    def test_validate_checks_that_identity_fields_are_formatted_correctly(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id",
                "1631204621,usd,5,Purchase,aaaaaaa,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aabbbbbbbcccccc,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bCdEbb-bbbbbbbb-2222222-222222-23456,2021,09,09,16,aabbbbbbbcccccc,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123",
                "1631204621,usd,5,Purchase,1234,website,00000000-1111-2222-aaaa-bbbbbbbbbbbb,2021,09,09,16,aabbbbbbbcccccc,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123",
                "1631204621,usd,5,Purchase,c@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1111111111111111111111111111.111,website,bbbb-bbbbbbbbbbb2222222222222222,2021,09,09,16,aabbbbbbbcccccc,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,b_dEbb-bbbbbbbb-2222222-222222-2-123,2021,09,09,16,aabbbbbbbcccccc,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222-12341234,2021,09,09,16,aabbbbbbbcccccc,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,Z,2021,09,09,16,aabbbbbbbcccccc,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,bat.man.123",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 7")
        self.assertRegex(result, "Rows with errors: 7")
        self.assertRegex(result, "Valid rows: 0")
        self.assertRegex(result, "Line numbers with incorrect 'email' format: 2,4,5")
        self.assertRegex(
            result, "Line numbers with incorrect 'device_id' format: 3,5,6,7,8"
        )

    def test_validate_checks_that_other_fields_are_formatted_correctly(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id,browser_name,device_os,device_os_version,data_source_id",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aabbaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456",
                "16312046210,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456",
                "1631204621,12usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,1234:abcd::,Mozilla,fb.1.1558571054389.1098115397,not_valid.user.id,Chrome,Mac OS X,10.13.6,123456",
                "1631204621,usd,ten,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,abcd,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456",
                "1631204621,usd,5,  Purchase   ,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,w,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456",
                "  1631204621 ,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456",
                "1631204621,usd ,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,abcg:abdf::,Mozilla,fb.1.1558571054389.1098115397,123456abc,Chrome,Mac OS X,10.13.6,123456",
                "1631204621,usd, 5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,12345,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website ,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456,Chrome,Mac OS X,10.13.6,123456",
            ]
        )
        result = generate_from_body(body)
        self.assertRegex(result, "Total rows: 10")
        self.assertRegex(result, "Rows with errors: 9")
        self.assertRegex(result, "Valid rows: 1")
        self.assertRegex(result, "Line numbers with incorrect 'timestamp' format: 3,8")
        self.assertRegex(
            result, "Line numbers with incorrect 'currency_type' format: 4,9"
        )
        self.assertRegex(
            result, "Line numbers with incorrect 'conversion_value' format: 5,10"
        )
        self.assertRegex(result, "Line numbers with incorrect 'event_type' format: 6")
        self.assertRegex(
            result, "Line numbers with incorrect 'action_source' format: 7,11"
        )
        self.assertRegex(result, "Line numbers with incorrect 'phone' format: 5,10")
        self.assertRegex(
            result, "Line numbers with incorrect 'client_ip_address' format: 3,9"
        )
        self.assertRegex(result, "Line numbers with incorrect 'login_id' format: 4,9")

    def test_validate_the_line_ending_cannot_be_in_dos_format(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id\r\n",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456\r\n",
            ]
        )
        result = generate_from_body(body)

        self.assertRegex(result, "ERROR - The CSV file is not valid.")
        self.assertRegex(result, r"Lines must end with a newline character: \\n")
        self.assertRegex(result, "The error was detected on line number: 1")
        self.assertRegex(result, "Validation processing stopped.")

    def test_validate_the_line_ending_cannot_contain_a_space(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id\n",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456 \n",
            ]
        )
        result = generate_from_body(body)

        self.assertRegex(result, "ERROR - The CSV file is not valid.")
        self.assertRegex(result, r"Lines must end with a newline character: \\n")
        self.assertRegex(result, "The error was detected on line number: 2")
        self.assertRegex(result, "Validation processing stopped.")

    def test_validate_the_line_ending_cannot_be_a_carriage_return(self):
        body = Mock("body")
        body.iter_lines = self.mock_lines_helper(
            [
                "timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour,phone,client_ip_address,client_user_agent,click_id,login_id\n",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456\n",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456\r",
                "1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbb2222222222222222,2021,09,09,16,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,10.0.0.1,Mozilla,fb.1.1558571054389.1098115397,123456",
            ]
        )
        result = generate_from_body(body)

        self.assertRegex(result, "ERROR - The CSV file is not valid.")
        self.assertRegex(result, r"Lines must end with a newline character: \\n")
        self.assertRegex(result, "The error was detected on line number: 3")
        self.assertRegex(result, "Validation processing stopped.")

    def mock_lines_helper(self, lines: List[str]) -> Mock:
        encoded_lines = list(map(lambda line: line.encode("utf-8"), lines))
        return Mock(return_value=encoded_lines)
