#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import time

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.entity.mpc_instance import MPCInstanceStatus, MPCParty
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.pid.entity.pid_instance import (
    PIDInstance,
    PIDInstanceStatus,
    PIDProtocol,
    PIDRole,
    PIDStageStatus,
)
from fbpcs.pid.entity.pid_stages import UnionPIDStage
from fbpcs.post_processing_handler.post_processing_handler import (
    PostProcessingHandlerStatus,
)
from fbpcs.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
    PostProcessingInstanceStatus,
)
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.infra_config import InfraConfig
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationGameType,
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)

LIFT_PC_PATH: str = os.path.join(
    os.path.dirname(__file__),
    "test_resources",
    "serialized_instances",
    "lift_pc_instance.json",
)
LIFT_PID_PATH: str = os.path.join(
    os.path.dirname(__file__),
    "test_resources",
    "serialized_instances",
    "lift_pid_instance.json",
)
LIFT_MPC_PATH: str = os.path.join(
    os.path.dirname(__file__),
    "test_resources",
    "serialized_instances",
    "lift_mpc_instance.json",
)


def gen_dummy_container_instance() -> ContainerInstance:
    """Creates a dummy container instance to be used in unit tests"""

    return ContainerInstance(
        instance_id="arn:aws:ecs:us-west-2:000000000000:task/cluster-name/subnet",
        status=ContainerInstanceStatus.COMPLETED,
        ip_address="10.0.10.242",
    )


def gen_dummy_pid_instance() -> PIDInstance:
    """Creates a dummy pid instance to be used in unit tests"""

    return PIDInstance(
        instance_id="pid_instance_id",
        protocol=PIDProtocol.UNION_PID,
        pid_role=PIDRole.PUBLISHER,
        num_shards=1,
        input_path="https://bucket.s3.us-west-2.amazonaws.com/lift/partner/partner_e2e_input.csv",
        output_path="https://bucket.s3.us-west-2.amazonaws.com/lift/partner/partner_instance_1638998680_0_out_dir/pid_stage/out.csv",
        status=PIDInstanceStatus.COMPLETED,
        data_path="",
        spine_path="",
        hmac_key="",
        stages_containers={
            UnionPIDStage.PUBLISHER_SHARD: [gen_dummy_container_instance()],
            UnionPIDStage.PUBLISHER_PREPARE: [gen_dummy_container_instance()],
            UnionPIDStage.PUBLISHER_RUN_PID: [gen_dummy_container_instance()],
        },
        stages_status={
            UnionPIDStage.PUBLISHER_SHARD: PIDStageStatus.COMPLETED,
            UnionPIDStage.PUBLISHER_PREPARE: PIDStageStatus.COMPLETED,
            UnionPIDStage.PUBLISHER_RUN_PID: PIDStageStatus.COMPLETED,
        },
        current_stage=UnionPIDStage.PUBLISHER_RUN_PID,
        server_ips=["10.0.10.242"],
    )


def gen_dummy_mpc_instance() -> PCSMPCInstance:
    """Creates a dummy mpc instance to be used in unit tests"""

    return PCSMPCInstance.create_instance(
        instance_id="mpc_instance_id",
        game_name="lift",
        mpc_party=MPCParty.SERVER,
        num_workers=1,
        server_ips=["10.0.10.242"],
        containers=[gen_dummy_container_instance()],
        status=MPCInstanceStatus.COMPLETED,
        game_args=[
            {
                "input_base_path": "https://bucket.s3.us-west-2.amazonaws.com/lift/partner/partner_instance_1638998680_0_out_dir/data_processing_stage/out.csv",
                "output_base_path": "https://bucket.s3.us-west-2.amazonaws.com/lift/partner/partner_instance_1638998680_0_out_dir/compute_stage/out.json",
                "num_files": 40,
                "concurrency": 4,
                "file_start_index": 0,
            }
        ],
    )


def gen_dummy_post_processing_instance() -> PostProcessingInstance:
    """Creates a dummy post processing instance to be used in unit tests"""
    return PostProcessingInstance(
        instance_id="post_processing_instance_id",
        handler_statuses={"handler1": PostProcessingHandlerStatus.COMPLETED},
        status=PostProcessingInstanceStatus.COMPLETED,
    )


def gen_dummy_pc_instance() -> PrivateComputationInstance:
    """Creates a dummy private computation instance to be used in unit tests"""
    infra_config: InfraConfig = InfraConfig(
        instance_id="pc_instance_id",
        role=PrivateComputationRole.PUBLISHER,
        status=PrivateComputationInstanceStatus.POST_PROCESSING_HANDLERS_COMPLETED,
        status_update_ts=int(time.time()),
        instances=[
            gen_dummy_pid_instance(),
            gen_dummy_mpc_instance(),
            gen_dummy_post_processing_instance(),
        ],
    )
    return PrivateComputationInstance(
        infra_config,
        num_files_per_mpc_container=40,
        game_type=PrivateComputationGameType.LIFT,
        input_path="https://bucket.s3.us-west-2.amazonaws.com/lift/partner/partner_e2e_input.csv",
        output_dir="https://bucket.s3.us-west-2.amazonaws.com/lift/partner",
        num_pid_containers=1,
        num_mpc_containers=1,
        attribution_rule=None,
        aggregation_type=None,
        retry_counter=0,
        hmac_key="",
        concurrency=4,
        padding_size=25,
        k_anonymity_threshold=100,
        _stage_flow_cls_name="PrivateComputationStageFlow",
        breakdown_key=BreakdownKey.get_default_key(),
        pce_config=PCEConfig(
            subnets=["subnet"],
            cluster="onedocker-cluster-name",
            region="us-west-2",
            onedocker_task_definition="arn:aws:ecs:us-west-2:000000000000:task/cluster-name/subnet",
        ),
    )


if __name__ == "__main__":
    for path, instance in zip(
        (LIFT_PID_PATH, LIFT_MPC_PATH, LIFT_PC_PATH),
        (gen_dummy_pid_instance(), gen_dummy_mpc_instance(), gen_dummy_pc_instance()),
    ):
        json_output = instance.dumps_schema()
        with open(path, "w") as f:
            f.write(json_output)
