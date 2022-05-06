#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.onedocker_binary_config import OneDockerBinaryConfig


def get_cmd_args(
    input_path: str, region: str, binary_config: OneDockerBinaryConfig
) -> str:
    return " ".join(
        [
            f"--input-file-path={input_path}",
            "--cloud-provider=AWS",
            f"--region={region}",
            # pc_pre_validation assumes all other binaries runs on the same version tag as its own
            f"--binary-version={binary_config.binary_version}",
        ]
    )
