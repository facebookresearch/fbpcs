#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import sys
from typing import Any, Dict, TextIO

import termcolor


def colored(s: str, *args: Any, outf: TextIO = sys.stdout, **kwargs: Any) -> str:
    """
    Calls termcolor.colored iff the outf is a TTY device.
    """
    if outf.isatty():
        return termcolor.colored(s, *args, **kwargs)
    return s
