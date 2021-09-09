#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""
CLI tool to generate a list of IDs from a spine

Usage:
    gen_ids_from_spine <spine_path> <output_path> [options]

Options:
    -h --help                   Show this help
    -r --keep_rate=<r>          Probability to keep a given input row [default: 0.6]
    --log_every_n=<n>           Output progress every N lines
"""

import os
import pathlib
import random
from typing import Any, Dict

import docopt
import schema


def gen_ids_from_spine(args: Dict[str, Any]) -> None:
    with open(args["<spine_path>"]) as f_in, open(args["<output_path>"], "w") as f_out:
        for i, line in enumerate(f_in):
            if random.random() < args["--keep_rate"]:
                f_out.write(line)
            lines_processed = i + 1
            if args["--log_every_n"] and lines_processed % args["--log_every_n"] == 0:
                print(f"Processed {lines_processed} lines")


def main():
    args_schema = schema.Schema(
        {
            "<spine_path>": schema.Use(pathlib.Path, os.path.exists),
            "<output_path>": schema.Use(pathlib.Path),
            "--keep_rate": schema.Use(float),
            schema.Optional("--log_every_n"): schema.Or(None, schema.Use(int)),
            "--help": bool,
        }
    )
    args = args_schema.validate(docopt.docopt(__doc__))
    gen_ids_from_spine(args)


if __name__ == "__main__":
    main()
