#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Legacy CLI for running a Private Lift study. This exists only because the run_fbpcs.sh script some advertisers have
still calls this CLI and we would like to request them to update run_fbpcs.sh through this warning message.
"""


if __name__ == "__main__":
    print(
        "Hello! It looks like you're running an outdated version of run_fbpcs.sh. Please follow these steps:\n"
        + '1. Run this command on your terminal to get the new run_fbpcs.sh: "curl -O https://raw.githubusercontent.com/facebookresearch/fbpcs/main/fbpcs/scripts/run_fbpcs.sh && chmod +x run_fbpcs.sh"\n'
        + '2. Modify the run_fbpcs command you ran by removing "lift" right after "run_fbpcs"\n'
        + "3. Run the modified run_fbpcs command. You should be good!"
    )
