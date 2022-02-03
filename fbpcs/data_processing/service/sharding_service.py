#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import enum
import os
import pathlib
from typing import Optional

from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


CPP_SHARDER_PATH = pathlib.Path(os.environ.get("CPP_SHARDER_PATH", os.getcwd()))
CPP_SHARDER_HASHED_FOR_PID_PATH = pathlib.Path(
    os.environ.get("CPP_SHARDER_HASHED_FOR_PID_PATH", "cpp_bin/sharder_hashed_for_pid")
)

# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


class ShardType(enum.Enum):
    ROUND_ROBIN = 1
    HASHED_FOR_PID = 2


class ShardingService(RunBinaryBaseService):
    @staticmethod
    def build_args(
        filepath: str,
        output_base_path: str,
        file_start_index: int,
        num_output_files: int,
        tmp_directory: str = "/tmp/",
        hmac_key: Optional[str] = None,
    ) -> str:
        cmd_args = " ".join(
            [
                f"--input_filename={filepath}",
                f"--output_base_path={output_base_path}",
                f"--file_start_index={file_start_index}",
                f"--num_output_files={num_output_files}",
                f"--tmp_directory={tmp_directory}",
            ]
        )

        if hmac_key:
            cmd_args += f" --hmac_base64_key={hmac_key}"

        return cmd_args

    @staticmethod
    def get_binary_name(
        shard_type: ShardType,
    ):
        # TODO: Probably put exe in an env variable?
        # Try to align with existing paths
        if shard_type is ShardType.ROUND_ROBIN:
            return OneDockerBinaryNames.SHARDER.value
        elif shard_type is ShardType.HASHED_FOR_PID:
            return OneDockerBinaryNames.SHARDER_HASHED_FOR_PID.value
        else:
            raise RuntimeError(f"Unsupported ShardType passed: {shard_type}")
