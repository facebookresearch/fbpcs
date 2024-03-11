#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import functools
import warnings


def deprecated_msg(msg: str):
    warning_color = "\033[93m"  # orange/yellow ascii escape sequence
    end = "\033[0m"  # end ascii escape sequence
    warnings.simplefilter("always", DeprecationWarning)
    warnings.warn(
        f"{warning_color}{msg}{end}",
        category=DeprecationWarning,
        stacklevel=2,
    )
    warnings.simplefilter("default", DeprecationWarning)


def deprecated(reason: str):
    """
    Logs a warning that a function is deprecated
    """

    def wrap(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            deprecated_msg(msg=f"{func.__name__} is deprecated! explanation: {reason}")
            return func(*args, **kwargs)

        return wrapped

    return wrap
