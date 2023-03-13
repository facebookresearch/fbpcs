#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)


def get_cmd_args(
    input_path: str,
    region: str,
    binary_config: OneDockerBinaryConfig,
    pre_validation_file_stream_flag: bool,
    publisher_pc_pre_validation_flag: bool,
    input_path_start_ts: Optional[str],
    input_path_end_ts: Optional[str],
    private_computation_role: Optional[PrivateComputationRole] = None,
) -> str:
    args = [
        f"--input-file-path={input_path}",
        "--cloud-provider=AWS",
        f"--region={region}",
        # pc_pre_validation assumes all other binaries runs on the same version tag as its own
        f"--binary-version={binary_config.binary_version}",
        f"--private-computation-role={private_computation_role}",
    ]

    if input_path_start_ts and input_path_end_ts:
        args.extend(
            [
                f"--start-timestamp={input_path_start_ts}",
                f"--end-timestamp={input_path_end_ts}",
            ]
        )

    if pre_validation_file_stream_flag:
        args.append("--pre-validation-file-stream=enabled")

    if publisher_pc_pre_validation_flag:
        args.append("--publisher-pc-pre-validation=enabled")

    return " ".join(args)
