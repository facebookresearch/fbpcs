#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from typing import Optional

from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


class PIDPrepareBinaryService(RunBinaryBaseService):
    @staticmethod
    def build_args(
        input_path: str,
        output_path: str,
        tmp_directory: str = "/tmp/",
        max_column_count: int = 1,
        run_id: Optional[str] = None,
    ) -> str:
        cmd_args = " ".join(
            [
                f"--input_path={input_path}",
                f"--output_path={output_path}",
                f"--tmp_directory={tmp_directory}",
                f"--max_column_cnt={max_column_count}",
            ]
        )
        if run_id is not None:
            cmd_args = " ".join(
                [
                    cmd_args,
                    f"--run_id={run_id}",
                ]
            )
        return cmd_args

    @staticmethod
    def get_binary_name() -> str:
        return OneDockerBinaryNames.UNION_PID_PREPARER.value
