#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import pathlib
import unittest
from unittest.mock import call, mock_open, patch

from fbpcs.scripts import gen_fake_data


class TestGenFakeData(unittest.TestCase):
    def test_gen_adjusted_purchase_rate(self):
        # test user - Within bounds
        res = gen_fake_data._gen_adjusted_purchase_rate(
            is_test=True, purchase_rate=0.9, incrementality_rate=0.2
        )
        self.assertEquals(1.0, res)

        # test user - exceed upper bound; except error
        with self.assertRaises(ValueError):
            res = gen_fake_data._gen_adjusted_purchase_rate(
                is_test=True, purchase_rate=0.9, incrementality_rate=0.4
            )

        # control user - Within bounds
        res = gen_fake_data._gen_adjusted_purchase_rate(
            is_test=False, purchase_rate=0.1, incrementality_rate=0.2
        )
        self.assertEquals(0.0, res)

        # control user - exceed lower bound; except error
        with self.assertRaises(ValueError):
            res = gen_fake_data._gen_adjusted_purchase_rate(
                is_test=False, purchase_rate=0.1, incrementality_rate=0.3
            )

    def test_faked_data(self):
        header = [
            gen_fake_data.InputColumn.id_,
            gen_fake_data.InputColumn.row_count,
            gen_fake_data.InputColumn.opportunity,
            gen_fake_data.InputColumn.test_flag,
            gen_fake_data.InputColumn.opportunity_timestamp,
            gen_fake_data.InputColumn.event_timestamp,
            gen_fake_data.InputColumn.value,
            gen_fake_data.InputColumn.purchase_flag,
        ]

        row_num = 123
        o_rate = 0.1
        t_rate = 0.2
        p_rate = 0.3
        i_rate = 0.0
        min_ts = 555
        max_ts = 555
        # Not used inside _faked_data() because header does not include
        # InputColumn.event_timestamps and InputColumn.values
        num_convs = 1
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
        )

        # Basic tests
        # id_ and row_count columns should be equal to row_num
        self.assertEqual(len(header), len(res))
        self.assertEqual(row_num, res[header.index(gen_fake_data.InputColumn.id_)])
        self.assertEqual(
            row_num, res[header.index(gen_fake_data.InputColumn.row_count)]
        )

        o_rate = 1.0
        t_rate = 0.0
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
        )
        # opportunity flag should be set and test flag should be unset
        self.assertEqual(1, res[header.index(gen_fake_data.InputColumn.opportunity)])
        self.assertEqual(0, res[header.index(gen_fake_data.InputColumn.test_flag)])
        self.assertEqual(
            min_ts, res[header.index(gen_fake_data.InputColumn.opportunity_timestamp)]
        )

        t_rate = 1.0
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
        )
        # opportunity flag should be set and test flag should also be set
        self.assertEqual(1, res[header.index(gen_fake_data.InputColumn.opportunity)])
        self.assertEqual(1, res[header.index(gen_fake_data.InputColumn.test_flag)])
        self.assertEqual(
            min_ts, res[header.index(gen_fake_data.InputColumn.opportunity_timestamp)]
        )

        o_rate = 0.0
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
        )
        # even though t_rate = 1.0, if opp is unset, test_flag is unset
        self.assertEqual(0, res[header.index(gen_fake_data.InputColumn.opportunity)])
        self.assertEqual(0, res[header.index(gen_fake_data.InputColumn.test_flag)])
        self.assertEqual(
            0, res[header.index(gen_fake_data.InputColumn.opportunity_timestamp)]
        )

        p_rate = 1.0
        # Expect a purchase
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
        )
        self.assertEqual(1, res[header.index(gen_fake_data.InputColumn.purchase_flag)])
        self.assertEqual(
            min_ts, res[header.index(gen_fake_data.InputColumn.event_timestamp)]
        )
        self.assertLess(0, res[header.index(gen_fake_data.InputColumn.value)])

        p_rate = 0.0
        # Expect no purchase
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
        )
        self.assertEqual(0, res[header.index(gen_fake_data.InputColumn.opportunity)])
        self.assertEqual(
            0, res[header.index(gen_fake_data.InputColumn.event_timestamp)]
        )
        self.assertEqual(0, res[header.index(gen_fake_data.InputColumn.value)])

        # Test with md5 IDs
        header = [gen_fake_data.InputColumn.id_]
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
            md5_id=True,
        )
        self.assertEqual(
            gen_fake_data._get_md5_hash_of_int(row_num),
            res[header.index(gen_fake_data.InputColumn.id_)],
        )

        # Test to ensure we're not outputting all columns
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
        )
        self.assertEqual(len(header), len(res))
        self.assertEqual(row_num, res[header.index(gen_fake_data.InputColumn.id_)])

        # Test with capping to 2 conversions per user
        num_convs = 2
        header = [
            gen_fake_data.InputColumn.id_,
            gen_fake_data.InputColumn.event_timestamps,
            gen_fake_data.InputColumn.values,
        ]
        res = gen_fake_data._faked_data(
            row_num=row_num,
            header=header,
            opportunity_rate=o_rate,
            test_rate=t_rate,
            purchase_rate=p_rate,
            incrementality_rate=i_rate,
            min_ts=min_ts,
            max_ts=max_ts,
            num_conversions=num_convs,
        )
        self.assertEqual(
            num_convs,
            len(res[header.index(gen_fake_data.InputColumn.event_timestamps)]),
        )
        self.assertEqual(
            num_convs, len(res[header.index(gen_fake_data.InputColumn.values)])
        )

    def test_generate_line(self):
        # Basic test
        header = [gen_fake_data.InputColumn.id_]
        line = "123"
        res = gen_fake_data._generate_line(
            row_num=100,
            line=line,
            header=header,
            opportunity_rate=0,
            test_rate=0,
            purchase_rate=0,
            incrementality_rate=0,
            min_ts=0,
            max_ts=1000,
            num_conversions=4,
        )
        self.assertEqual(len(header), len(res))
        self.assertEqual("123", res[0])

        # Try with no input line at all
        header = [gen_fake_data.InputColumn.id_]
        line = ""
        res = gen_fake_data._generate_line(
            row_num=100,
            line=line,
            header=header,
            opportunity_rate=0,
            test_rate=0,
            purchase_rate=0,
            incrementality_rate=0,
            min_ts=0,
            max_ts=1000,
            num_conversions=4,
        )
        self.assertEqual(len(header), len(res))
        self.assertEqual("100", res[0])

        # Line with multiple overrides
        header = [
            gen_fake_data.InputColumn.id_,
            gen_fake_data.InputColumn.opportunity,
            gen_fake_data.InputColumn.test_flag,
        ]
        line = "222,1"
        res = gen_fake_data._generate_line(
            row_num=999,
            line=line,
            header=header,
            opportunity_rate=1.0,
            test_rate=1.0,
            purchase_rate=1.0,
            incrementality_rate=0,
            min_ts=0,
            max_ts=1000,
            num_conversions=4,
        )
        self.assertEqual(len(header), len(res))
        self.assertEqual("222", res[header.index(gen_fake_data.InputColumn.id_)])
        self.assertEqual("1", res[header.index(gen_fake_data.InputColumn.opportunity)])
        self.assertEqual("1", res[header.index(gen_fake_data.InputColumn.test_flag)])

        # Check that min_ts/max_ts is limiting the valid values
        header = [
            gen_fake_data.InputColumn.id_,
            gen_fake_data.InputColumn.opportunity_timestamp,
        ]
        line = "123"
        res = gen_fake_data._generate_line(
            row_num=100,
            line=line,
            header=header,
            opportunity_rate=1.0,
            test_rate=1.0,
            purchase_rate=1.0,
            incrementality_rate=0,
            min_ts=555,
            max_ts=555,
            num_conversions=4,
        )
        self.assertEqual(len(header), len(res))
        self.assertEqual("123", res[0])
        self.assertEqual("555", res[1])

    def test_make_input_csv(self):
        # First test without --num_records
        input_lines = ["id_,opportunity,test_flag", "1", "2", "3", "4", "5"]
        input_text = "\n".join(input_lines)
        args = {
            "<input_path>": pathlib.Path("/tmp/fake_input.csv"),
            "<output_path>": pathlib.Path("/tmp/fake_output.csv"),
            "--opportunity_rate": 0.0,
            "--test_rate": 0.0,
            "--purchase_rate": 1.0,
            "--incrementality_rate": 0.0,
            "--min_ts": 555,
            "--max_ts": 555,
            "--md5_id": False,
            "--num_conversions": 4,
        }

        m = mock_open(read_data=input_text)
        with patch("builtins.open", m):
            gen_fake_data._make_input_csv(args)

        # Check that readline was called once more than the end of the input
        self.assertEqual(len(input_lines) + 1, m().readline.call_count)
        # We've hard-coded opportunity rate above to zero
        m().write.assert_has_calls(
            [
                call("id_,opportunity,test_flag\n"),
                call("1,0,0\n"),
                call("2,0,0\n"),
                call("3,0,0\n"),
                call("4,0,0\n"),
                call("5,0,0\n"),
            ]
        )

        # Now test with some data given and --num_records set higher
        m.reset_mock()
        args["--num_records"] = 10
        m = mock_open(read_data=input_text)
        with patch("builtins.open", m):
            gen_fake_data._make_input_csv(args)
        self.assertEqual(args["--num_records"] + 1, m().readline.call_count)
        m().write.assert_has_calls(
            [
                call("id_,opportunity,test_flag\n"),
                call("1,0,0\n"),
                call("2,0,0\n"),
                call("3,0,0\n"),
                call("4,0,0\n"),
                call("5,0,0\n"),
            ]
        )
        # Check for additional writes (remember our lines are 0-indexed)
        m().write.assert_has_calls([call(f"{i},0,0\n") for i in range(5, 10)])

        # Finally, test with --num_records set and no input data
        input_text = "id_,opportunity,test_flag"
        m = mock_open(read_data=input_text)
        with patch("builtins.open", m):
            gen_fake_data._make_input_csv(args)
        self.assertEqual(args["--num_records"] + 1, m().readline.call_count)
        calls = [call("id_,opportunity,test_flag\n")]
        for i in range(args["--num_records"]):
            calls.append(call(f"{i},0,0\n"))
        m().write.assert_has_calls(calls)

        # Test with --from_header and --num_records set
        m.reset_mock()
        args["--num_records"] = 10
        args["--from_header"] = "id_,opportunity,test_flag"
        args["<input_path>"] = None
        m = mock_open()
        with patch("builtins.open", m):
            gen_fake_data._make_input_csv(args)

        calls = [call("id_,opportunity,test_flag\n")]
        for i in range(args["--num_records"]):
            calls.append(call(f"{i},0,0\n"))
        m().write.assert_has_calls(calls)
