#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""
CLI tool to simplify the generation of the config.yml file

Usage:
    gen_config <input_path> [options]
    gen_config <input_path> <new_output_path> [options]

Options:
    -r --replace=<k>    Key to traverse for replacement [default: TODO]
    -a --accept_all     Do not re-prompt if we already have an accepted input for the given key
    -h --help           Show this help text
"""

import os
import pathlib
from typing import Any, Dict, Optional

import docopt
import schema
from fbpcs.util import yaml


def prompt(key: str, replacements: Dict[str, str], accept_all: bool = False) -> str:
    prompt_key: str
    if key in replacements:
        if accept_all:
            return replacements[key]

        replacement = replacements[key]
        prompt_key = f"{key} ({replacement}): "
    else:
        prompt_key = f"{key}: "

    response = input(prompt_key)
    if len(response) == 0:
        response = replacements.get(key, "")
    return response


def update_dict(
    d: Dict[str, Any],
    replace_key: str,
    replacements: Optional[Dict[str, str]] = None,
    accept_all: bool = False,
) -> None:
    # Funny case where we *can't* use default Python coalescing (var or {})
    # because the truthiness of an empty dict is False, which means it would
    # instantiate a NEW empty dict, giving each recursive call a new dict.
    # Instead we must explicitly check for None.
    if replacements is None:
        replacements = {}

    # Assume we can only update leaf nodes, otherwise things get weird
    for k, v in d.items():
        if isinstance(v, str) and v == replace_key:
            new_value = prompt(k, replacements, accept_all)
            replacements[k] = new_value
            d[k] = new_value
        elif isinstance(v, list) and replace_key in v:
            new_value = prompt(k, replacements, accept_all)
            replacements[k] = new_value
            d[k] = new_value.split(",")
        elif isinstance(v, dict):
            # Recurse
            update_dict(v, replace_key, replacements, accept_all)


def gen_config(args: Dict[str, Any]) -> None:
    config = yaml.load(args["<input_path>"])
    update_dict(config, args["--replace"], None, args["--accept_all"])

    # Coalesce: output to new_output_path if present, otherwise overwrite
    output_path = args["<new_output_path>"] or args["<input_path>"]
    yaml.dump(config, output_path)


def main():
    args_schema = schema.Schema(
        {
            "<input_path>": schema.Use(pathlib.Path, os.path.exists),
            "<new_output_path>": schema.Or(
                None, schema.Use(pathlib.Path, os.path.exists)
            ),
            "--replace": str,
            "--accept_all": bool,
            "--help": bool,
        }
    )
    args = args_schema.validate(docopt.docopt(__doc__))
    gen_config(args)


if __name__ == "__main__":
    main()
