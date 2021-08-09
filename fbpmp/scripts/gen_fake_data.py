#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""
CLI tool to generate fake data for testing MPC

Usage:
    gen_fake_data <output_path> [options]
    gen_fake_data <input_path> <output_path> [options]

Options:
    -h --help                     Show this help
    -n --num_records=<n>          Number of records to emit (default: detect from input)
    -or --opportunity_rate=<o>    Probability of a given publisher row having an opportunity [default: 0.8]
    -tr --test_rate=<t>           Probability of a given publisher row being logged to test [default: 0.5]
    -pr --purchase_rate=<p>       Probability of a given partner row having a purchase [default: 0.2]
    -ir --incrementality_rate=<i> Desired incrementality rate from the faked data [default: 0.0]
    --min_ts=<t>                  Minimum timestamp for opportunity_timestamp/event_timestamp [default: 1600000000]
    --max_ts=<t>                  Maximum timestamp for opportunity_timestamp/event_timestamp [default: 1600001000]
    --md5_id                      Use md5 hashes for ID column instead of integers
    --num_conversions=<n>         Number of event timestamps and values per partner row [default: 4]
    -f --from_header=<hdr>        Comma-separated list of header columns, used instead of input file if input file is not supplied
"""

import hashlib
import os
import pathlib
import random
from typing import Any, Dict, List, Union

import docopt
import schema
from fbpmp.pcf.structs import InputColumn


def _get_md5_hash_of_int(i: int) -> str:
    return hashlib.md5(bytes(str(i), encoding="utf-8")).hexdigest()


def _gen_adjusted_purchase_rate(
    is_test: bool, purchase_rate: float, incrementality_rate: float
) -> float:

    if is_test:
        adj_purchase_rate = purchase_rate + (incrementality_rate / 2.0)
        if adj_purchase_rate > 1.0:
            raise ValueError(
                ">1.0 incrementality_rate + purchase_rate is not yet supported"
            )
    else:
        adj_purchase_rate = purchase_rate - (incrementality_rate / 2.0)
        if adj_purchase_rate < 0.0:
            raise ValueError(
                "Incrementality rate cannot be significantly higher than the purchase rate"
            )
    return adj_purchase_rate


def _faked_data(
    row_num: int,
    header: List[InputColumn],
    opportunity_rate: float,
    test_rate: float,
    purchase_rate: float,
    incrementality_rate: float,
    min_ts: int,
    max_ts: int,
    num_conversions: int,
    md5_id: bool = False,
) -> List[Union[str, Any]]:
    has_opp = 1 if random.random() < opportunity_rate else 0
    is_test = 1 if has_opp and random.random() < test_rate else 0

    purchase_rate = _gen_adjusted_purchase_rate(
        bool(is_test), purchase_rate, incrementality_rate
    )

    has_purchase = 1 if random.random() < purchase_rate else 0
    value = has_purchase * random.randint(1, 100)
    rand_data = {
        InputColumn.id_: _get_md5_hash_of_int(row_num) if md5_id else row_num,
        InputColumn.row_count: row_num,
        InputColumn.opportunity: has_opp,
        InputColumn.test_flag: is_test,
        InputColumn.opportunity_timestamp: has_opp * random.randint(min_ts, max_ts),
        # opportunity_timestamps is an array of values for each data row
        # For testing we can assume an array of size 5
        InputColumn.opportunity_timestamps: [
            has_opp * random.randint(min_ts, max_ts) for _ in range(5)
        ],
        InputColumn.event_timestamp: has_purchase * random.randint(min_ts, max_ts),
        # event_timestamps can be an array of all zeros, valid timestamps preceded
        # by zeroes, or all non-zeroes
        InputColumn.event_timestamps: sorted(
            [
                has_purchase * random.randint(min_ts, max_ts)
                for _ in range(row_num % num_conversions + 1)
            ]
            + [0] * (num_conversions - row_num % num_conversions - 1)
        ),
        InputColumn.value: value,
        # values can be an array of all zeros, non-zeroes preceded
        # by zeroes, or all non-zeroes. The number of non-zeroes would
        # match that of the event_timestamps column.
        InputColumn.values: [
            has_purchase * random.randint(1, 100)
            if i >= (num_conversions - row_num % num_conversions - 1)
            else 0
            for i in range(num_conversions)
        ],
        InputColumn.value_squared: value * value,
        InputColumn.purchase_flag: has_purchase,
        # For now, assume feature columns are all binary
        InputColumn.features: random.randint(0, 1),
    }
    return [rand_data[column] for column in header]


def _generate_line(
    row_num: int,
    line: str,
    header: List[InputColumn],
    opportunity_rate: float,
    test_rate: float,
    purchase_rate: float,
    incrementality_rate: float,
    min_ts: int,
    max_ts: int,
    num_conversions: int,
    md5_id: bool = False,
) -> List[str]:
    # first try to read data from the input file
    column_overrides = {}
    if line != "":
        values = line.split(",")
        column_overrides = {col: val for col, val in zip(header, values)}
    # Get some fake data
    values = _faked_data(
        row_num=row_num,
        header=header,
        opportunity_rate=opportunity_rate,
        test_rate=test_rate,
        purchase_rate=purchase_rate,
        incrementality_rate=incrementality_rate,
        min_ts=min_ts,
        max_ts=max_ts,
        num_conversions=num_conversions,
        md5_id=md5_id,
    )
    # Override with input data
    for column, value in column_overrides.items():
        values[header.index(column)] = value
    return [str(x) for x in values]


def _make_input_csv(args: Dict[str, Any]) -> None:

    if args.get("<input_path>") is None:
        header_line = args.get("--from_header").split(",")
        header = [InputColumn.from_str(s) for s in header_line]

        with open(args["<output_path>"], "w") as f_out:
            f_out.write(args.get("--from_header") + "\n")

            for i in range(args["--num_records"]):
                out_line = _generate_line(
                    i,
                    "",
                    header,
                    args["--opportunity_rate"],
                    args["--test_rate"],
                    args["--purchase_rate"],
                    args["--incrementality_rate"],
                    args["--min_ts"],
                    args["--max_ts"],
                    args["--num_conversions"],
                    args["--md5_id"],
                )
                f_out.write(",".join(out_line) + "\n")
    else:
        with open(args["<input_path>"]) as f_in, open(
            args["<output_path>"], "w"
        ) as f_out:
            header_line = f_in.readline().strip().split(",")
            header = [InputColumn.from_str(s) for s in header_line]
            f_out.write(",".join(header_line) + "\n")

            if args.get("--num_records") is not None:
                for i in range(args["--num_records"]):
                    line = f_in.readline().strip()
                    out_line = _generate_line(
                        i,
                        line,
                        header,
                        args["--opportunity_rate"],
                        args["--test_rate"],
                        args["--purchase_rate"],
                        args["--incrementality_rate"],
                        args["--min_ts"],
                        args["--max_ts"],
                        args["--num_conversions"],
                        args["--md5_id"],
                    )
                    f_out.write(",".join(out_line) + "\n")
            else:
                i = 0
                line = f_in.readline().strip()
                while line != "":
                    out_line = _generate_line(
                        i,
                        line,
                        header,
                        args["--opportunity_rate"],
                        args["--test_rate"],
                        args["--purchase_rate"],
                        args["--incrementality_rate"],
                        args["--min_ts"],
                        args["--max_ts"],
                        args["--num_conversions"],
                    )
                    f_out.write(",".join(out_line) + "\n")
                    line = f_in.readline().strip()
                    i += 1


def main():
    args_schema = schema.Schema(
        {
            schema.Optional("<input_path>"): schema.Or(
                None, schema.Use(pathlib.Path, os.path.exists)
            ),
            "<output_path>": schema.Use(pathlib.Path, os.path.exists),
            schema.Optional("--num_records"): schema.Or(None, schema.Use(int)),
            "--opportunity_rate": schema.Use(float),
            "--test_rate": schema.Use(float),
            "--purchase_rate": schema.Use(float),
            "--incrementality_rate": schema.Use(float),
            "--min_ts": schema.Use(int),
            "--max_ts": schema.Use(int),
            "--num_conversions": schema.Use(int),
            "--md5_id": bool,
            "--help": bool,
            schema.Optional("--from_header"): schema.Or(None, schema.Use(str)),
        }
    )
    args = args_schema.validate(docopt.docopt(__doc__))

    # verify input source arguments
    if args.get("<input_path>") is None and args.get("--from_header") is None:
        raise RuntimeError(
            "Missing input source, please supply either <input_path> or --from_header option"
        )

    if (
        args.get("<input_path>") is None
        and args.get("--from_header") is not None
        and args.get("--num_records") is None
    ):
        raise RuntimeError(
            "Missing argument, please specify --num_records with --from_header option"
        )

    _make_input_csv(args)


if __name__ == "__main__":
    main()
