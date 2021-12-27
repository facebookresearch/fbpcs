#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


if __name__ == "__main__":
    print(
        "*********************************************************************************************************\n"
        + "Hello! If you're a Meta internal who is running pa_coordinator, please run private_computation_cli instead.\n"
        + "Otherwise, it looks like you're running an outdated version of run_fbpcs.sh.\n"
        + "Please follow these steps:\n"
        + '1. Run this command on your terminal to get the new run_fbpcs.sh: "curl -O https://raw.githubusercontent.com/facebookresearch/fbpcs/main/fbpcs/scripts/run_fbpcs.sh && chmod +x run_fbpcs.sh"\n'
        + '2. Modify the run_fbpcs command you ran by removing "attribution" right after "run_fbpcs"\n'
        + '3. If you would like to run the "create_instance" command, make sure you have the argument "--game_type=attribution" in your command\n'
        + '4. If you would like to run the "compute_attribution" command, replace "compute_attribution" with "compute_metrics"\n'
        + "5. Run the modified run_fbpcs command. You should be good!\n"
        + "*********************************************************************************************************"
    )
