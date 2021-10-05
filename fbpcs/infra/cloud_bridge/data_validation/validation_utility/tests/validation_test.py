# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import re
from typing import List
from unittest import TestCase
from unittest.mock import Mock
from validation import generate_from_body, ONE_OR_MORE_REQUIRED_FIELDS, ALL_REQUIRED_FIELDS

class TestValidation(TestCase):

    def test_validate_requires_header_row(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper(['bad,header,row','1,2,3'])
        result = generate_from_body(body)
        expected_all_fields = ','.join(sorted(ALL_REQUIRED_FIELDS))
        expected_one_or_more_fields = ','.join(sorted(ONE_OR_MORE_REQUIRED_FIELDS))
        self.assertRegex(result, f'Header row not valid, missing `{expected_all_fields}` required fields')
        self.assertRegex(result, f'Header row not valid, at least one of `{expected_one_or_more_fields}` is required')
        self.assertRegex(result, 'Validation processing stopped.')

    def test_validate_returns_number_of_rows(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper([
            'timestamp,currency_type,conversion_value,event_type,email,action_source,year,month,day,hour',
            '1631204619,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,2021,09,09,16',
            '1631204619,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,2021,09,09,16',
        ])
        result = generate_from_body(body)
        self.assertRegex(result, 'Total rows: 2')
        self.assertRegex(result, 'Valid rows: 2')

    def test_validate_returns_validation_counts(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper([
            'timestamp,currency_type,conversion_value,event_type,email,action_source,year,month,day,hour',
            ',,,,,,,,,',
            '1631204619,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,2021,09,09,16',
            ',,,,,,,,,',
            ',,,,,,,,,',
            '1631204619,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,2021,09,09,16',
        ])
        result = generate_from_body(body)
        self.assertRegex(result, 'Total rows: 5')
        self.assertRegex(result, 'Valid rows: 2')
        self.assertRegex(result, 'Rows with errors: 3')

    def test_validate_fails_when_one_required_field_is_empty(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper([
            'timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour',
            ',usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204619,,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204619,usd,,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204619,usd,5,,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204619,usd,5,Purchase,,website,,2021,09,09,16',
            '1631204619,usd,5,,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
        ])
        result = generate_from_body(body)
        self.assertRegex(result, 'Total rows: 6')
        self.assertRegex(result, 'Rows with errors: 6')
        self.assertRegex(result, 'Valid rows: 0')

    def test_validate_handles_quoted_csvs(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper([
            '"timestamp","currency_type","conversion_value","event_type","email","action_source","year","month","day","hour"',
            '"","","","","","","","","",""',
            '"1631204619","usd","5","Purchase","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111","website","2021","09","09","16"',
            '"","","","","","","","","",""',
        ])
        result = generate_from_body(body)
        self.assertRegex(result, 'Total rows: 3')
        self.assertRegex(result, 'Valid rows: 1')
        self.assertRegex(result, 'Rows with errors: 2')

    def test_validate_reports_which_rows_are_missing_which_field(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper([
            'timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour',
            '1631204621,,,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            ',usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
        ])
        result = generate_from_body(body)
        self.assertRegex(result, 'Total rows: 4')
        self.assertRegex(result, 'Rows with errors: 3')
        self.assertRegex(result, 'Valid rows: 1')
        self.assertRegex(result, "Line numbers missing 'timestamp': 4")
        self.assertRegex(result, "Line numbers missing 'currency_type': 2,5")
        self.assertRegex(result, "Line numbers missing 'conversion_value': 2")
        self.assertRegex(result, "Line numbers missing 'action_source': 4,5")

    def test_validate_reports_which_rows_are_missing_all_identity_fields(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper([
            'timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,,website,,2021,09,09,16',
            '1631204621,usd,5,Purchase,,website,,2021,09,09,16',
            '1631204621,usd,5,Purchase,,website,,2021,09,09,16',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
        ])
        result = generate_from_body(body)
        expected_required_fields = ','.join(sorted(ONE_OR_MORE_REQUIRED_FIELDS))
        self.assertRegex(result, 'Total rows: 5')
        self.assertRegex(result, 'Rows with errors: 3')
        self.assertRegex(result, 'Valid rows: 2')
        self.assertRegex(result, f"Line numbers that are missing 1 or more of these required fields '{expected_required_fields}': 3,4,5")

    def test_validate_report_lists_only_the_first_100_missing_lines_per_error(self):
        body = Mock('body')
        lines = [
            'timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
        ]
        lines.extend(
            [',usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16'] * 100
        )
        lines.extend(
            ['1631204621,usd,5,,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16'] * 101
        )
        lines.extend(
            ['1631204621,usd,5,Purchase,,website,,2021,09,09,16'] * 200
        )
        body.iter_lines = self.mock_lines_helper(lines)
        result = generate_from_body(body)
        expected_lines_missing_timestamp = ','.join(map(str, range(3, 103)))
        expected_lines_missing_event_type = ','.join(map(str, range(103, 203)))
        expected_fields_all_missing = ','.join(sorted(ONE_OR_MORE_REQUIRED_FIELDS))
        expected_warning = r'\(First 100 lines shown\)'
        expected_lines_all_missing = ','.join(map(str, range(204,304)))
        self.assertRegex(result, 'Total rows: 402')
        self.assertRegex(result, 'Rows with errors: 401')
        self.assertRegex(result, 'Valid rows: 1')
        self.assertRegex(result, f"Line numbers missing 'timestamp': {expected_lines_missing_timestamp}")
        self.assertRegex(result, f"Line numbers missing 'event_type' {expected_warning}: {expected_lines_missing_event_type}")
        self.assertRegex(
            result,
            re.compile(
                f"Line numbers that are missing 1 or more of these required fields '{expected_fields_all_missing}' {expected_warning}: {expected_lines_all_missing}"
            )
        )

    def test_validate_checks_that_identity_fields_are_formatted_correctly(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper([
            'timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour',
            '1631204621,usd,5,Purchase,aaaaaaa,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,b-BbbbbbbbbbbbF-Abbbbbbbbbbbbbbb22-2222-222222222222-22222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,1234,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,c@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1111111111111111111111111111.111,website,b-BbbbbbbbbbbbF-Abbbbbbbbbbbbbbb22-2222-222222222222-22222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,b_BbbbbbbbbbbbF-Abbbbbbbbbbbbbbb22-2222-222222222222-22222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,10,2021,09,09,16',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,Z,2021,09,09,16',
        ])
        result = generate_from_body(body)
        self.assertRegex(result, 'Total rows: 7')
        self.assertRegex(result, 'Rows with errors: 6')
        self.assertRegex(result, 'Valid rows: 1')
        self.assertRegex(result, "Line numbers with incorrect 'email' format: 2,4,5")
        self.assertRegex(result, "Line numbers with incorrect 'device_id' format: 6,7,8")

    def test_validate_checks_that_other_fields_are_formatted_correctly(self):
        body = Mock('body')
        body.iter_lines = self.mock_lines_helper([
            'timestamp,currency_type,conversion_value,event_type,email,action_source,device_id,year,month,day,hour',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            'september-2021,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,12usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd,ten,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd,5,  Purchase   ,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,w,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '  1631204621 ,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd ,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd, 5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
            '1631204621,usd,5,Purchase,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111,website ,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222,2021,09,09,16',
        ])
        result = generate_from_body(body)
        self.assertRegex(result, 'Total rows: 10')
        self.assertRegex(result, 'Rows with errors: 9')
        self.assertRegex(result, 'Valid rows: 1')
        self.assertRegex(result, "Line numbers with incorrect 'timestamp' format: 3,8")
        self.assertRegex(result, "Line numbers with incorrect 'currency_type' format: 4,9")
        self.assertRegex(result, "Line numbers with incorrect 'conversion_value' format: 5,10")
        self.assertRegex(result, "Line numbers with incorrect 'event_type' format: 6")
        self.assertRegex(result, "Line numbers with incorrect 'action_source' format: 7,11")

    def mock_lines_helper(self, lines: List[str]) -> Mock:
        encoded_lines = list(map(lambda line: line.encode('utf-8'), lines))
        return Mock(return_value = encoded_lines)
