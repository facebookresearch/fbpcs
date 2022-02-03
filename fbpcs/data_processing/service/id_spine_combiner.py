#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import List, Optional

from fbpcp.util.arg_builder import build_cmd_args
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.private_computation.service.constants import DEFAULT_SORT_STRATEGY
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


class IdSpineCombinerService(RunBinaryBaseService):
    @staticmethod
    def build_args(
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        tmp_directory: str,
        sort_strategy: str = DEFAULT_SORT_STRATEGY,
        # TODO T106159008: padding_size and run_name are only temporarily optional
        # because Lift does not use them. It should and will be required to use them.
        padding_size: Optional[int] = None,
        # run_name is the binary name used by the log cost to s3 feature
        run_name: Optional[str] = None,
    ) -> List[str]:
        # TODO: Combiner could be made async so we don't have to spawn our
        # own ThreadPoolExecutor here and instead use async primitives
        cmd_args_list = []
        for shard in range(num_shards):
            # TODO: There's a weird dependency between these two services
            # AttributionIdSpineCombiner should operate independently of PIDStage
            next_spine_path = PIDStage.get_sharded_filepath(spine_path, shard)
            next_data_path = PIDStage.get_sharded_filepath(data_path, shard)
            next_output_path = PIDStage.get_sharded_filepath(output_path, shard)
            cmd_args = build_cmd_args(
                spine_path=next_spine_path,
                data_path=next_data_path,
                output_path=next_output_path,
                tmp_directory=tmp_directory,
                padding_size=padding_size,
                run_name=run_name,
                sort_strategy=sort_strategy,
            )
            cmd_args_list.append(cmd_args)
        return cmd_args_list
