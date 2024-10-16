#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import List, Optional

# pyre-fixme[21]: Could not find module `fbpcp.util.arg_builder`.
from fbpcp.util.arg_builder import build_cmd_args

# pyre-fixme[21]: Could not find module `fbpcs.private_computation.service.constants`.
from fbpcs.private_computation.service.constants import DEFAULT_SORT_STRATEGY

# pyre-fixme[21]: Could not find module `fbpcs.private_computation.service.pid_utils`.
from fbpcs.private_computation.service.pid_utils import get_sharded_filepath

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.service.run_binary_base_service`.
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


# pyre-fixme[11]: Annotation `RunBinaryBaseService` is not defined as a type.
class IdSpineCombinerService(RunBinaryBaseService):
    @staticmethod
    def build_args(
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        tmp_directory: str,
        protocol_type: str,
        max_id_column_cnt: int = 1,
        # pyre-fixme[16]: Module `fbpcs` has no attribute `private_computation`.
        sort_strategy: str = DEFAULT_SORT_STRATEGY,
        # TODO T106159008: padding_size and run_name are only temporarily optional
        # because Lift does not use them. It should and will be required to use them.
        padding_size: Optional[int] = None,
        multi_conversion_limit: Optional[int] = None,
        # run_name is the binary name used by the log cost to s3 feature
        run_name: Optional[str] = None,
        log_cost: Optional[bool] = False,
        run_id: Optional[str] = None,
        log_cost_bucket: Optional[str] = None,
    ) -> List[str]:
        # TODO: Combiner could be made async so we don't have to spawn our
        # own ThreadPoolExecutor here and instead use async primitives
        cmd_args_list = []
        for shard in range(num_shards):
            # pyre-fixme[16]: Module `fbpcs` has no attribute `private_computation`.
            next_spine_path = get_sharded_filepath(spine_path, shard)
            # pyre-fixme[16]: Module `fbpcs` has no attribute `private_computation`.
            next_data_path = get_sharded_filepath(data_path, shard)
            # pyre-fixme[16]: Module `fbpcs` has no attribute `private_computation`.
            next_output_path = get_sharded_filepath(output_path, shard)
            cmd_args = build_cmd_args(
                spine_path=next_spine_path,
                data_path=next_data_path,
                output_path=next_output_path,
                tmp_directory=tmp_directory,
                max_id_column_cnt=max_id_column_cnt,
                padding_size=padding_size,
                multi_conversion_limit=multi_conversion_limit,
                run_name=run_name,
                sort_strategy=sort_strategy,
                log_cost=log_cost,
                run_id=run_id,
                protocol_type=protocol_type,
                log_cost_s3_bucket=log_cost_bucket,
            )
            cmd_args_list.append(cmd_args)
        return cmd_args_list
