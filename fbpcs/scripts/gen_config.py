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
    -f --from=<other>   Copy values from another config
    -h --help           Show this help text
"""

import os
import pathlib
from typing import Any, Dict, Optional

import docopt
import schema
from fbpcp.util import yaml


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


def build_replacements_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    # Assume we can only update leaf nodes, otherwise things get weird
    replacements = {}
    for k, v in config.items():
        if isinstance(v, str):
            replacements[k] = v
        elif isinstance(v, list):
            replacements[k] = v
        elif isinstance(v, dict):
            # Recurse
            replacements.update(build_replacements_from_config(v))
    return replacements


def update_dict(
    d: Dict[str, Any],
    replace_key: str,
    replacements: Optional[Dict[str, Any]] = None,
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
            # Special case based on intent of writing [TODO] in a yaml file
            # We don't want to fill that in like [[subnet-123]] and accidentally
            # nest the array even further
            if len(v) == 1 and k in replacements:
                d[k] = replacements[k]
            else:
                # Honestly, we probably shouldn't allow something like this
                # This branch can only be hit in a case where a yaml file looks
                # like key: [val1, TODO, val2] which would be awkward
                new_value = prompt(k, replacements, accept_all)
                replacements[k] = new_value
                d[k] = new_value.split(",")
        elif isinstance(v, dict):
            # Recurse
            update_dict(v, replace_key, replacements, accept_all)


def gen_config(args: Dict[str, Any]) -> None:
    config = yaml.load(args["<input_path>"])
    replacements = {}
    if "--from" in args and args["--from"] is not None:
        other_config = yaml.load(args["--from"])
        replacements = build_replacements_from_config(other_config)
    update_dict(config, args["--replace"], replacements, args["--accept_all"])

    # Coalesce: output to new_output_path if present, otherwise overwrite
    output_path = args["<new_output_path>"] or args["<input_path>"]
    yaml.dump(config, output_path)


def main() -> None:
    args_schema = schema.Schema(
        {
            "<input_path>": schema.Use(pathlib.Path, os.path.exists),
            "<new_output_path>": schema.Or(
                None, schema.Use(pathlib.Path, os.path.exists)
            ),
            "--replace": str,
            "--accept_all": bool,
            "--from": schema.Or(None, str),
            "--help": bool,
        }
    )
    args = args_schema.validate(docopt.docopt(__doc__))
    gen_config(args)


if __name__ == "__main__":
    main()
