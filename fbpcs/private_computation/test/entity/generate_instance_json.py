#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import json
import os
import time

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)
from fbpcs.post_processing_handler.post_processing_handler import (
    PostProcessingHandlerStatus,
)
from fbpcs.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
    PostProcessingInstanceStatus,
)
from fbpcs.private_computation.entity.breakdown_key import BreakdownKey
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
)
from fbpcs.private_computation.entity.pce_config import PCEConfig
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.entity.product_config import (
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)

LIFT_PC_PATH: str = os.path.join(
    os.path.dirname(__file__),
    "test_resources",
    "serialized_instances",
    "lift_pc_instance.json",
)
STAGE_STATE_PATH: str = os.path.join(
    os.path.dirname(__file__),
    "test_resources",
    "serialized_instances",
    "stage_state_instance.json",
)


def gen_dummy_container_instance() -> ContainerInstance:
    """Creates a dummy container instance to be used in unit tests"""

    return ContainerInstance(
        instance_id="arn:aws:ecs:us-west-2:000000000000:task/cluster-name/subnet",
        status=ContainerInstanceStatus.COMPLETED,
        ip_address="10.0.10.242",
    )


def gen_dummy_stage_state_instance() -> StageStateInstance:
    """Creates a dummy stage state instance to be used in unit tests"""

    return StageStateInstance(
        instance_id="stage_state_instance_id",
        stage_name="PC_PRE_VALIDATION",
        status=StageStateInstanceStatus.COMPLETED,
        containers=[
            ContainerInstance(
                instance_id="test_container_instance_0",
                ip_address="1.1.1.1",
                status=ContainerInstanceStatus.COMPLETED,
            )
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
            gen_dummy_stage_state_instance(),
            gen_dummy_post_processing_instance(),
        ],
        game_type=PrivateComputationGameType.LIFT,
        pce_config=PCEConfig(
            subnets=["subnet"],
            cluster="onedocker-cluster-name",
            region="us-west-2",
            onedocker_task_definition="arn:aws:ecs:us-west-2:000000000000:task/cluster-name/subnet",
        ),
        _stage_flow_cls_name="PrivateComputationStageFlow",
        retry_counter=0,
        num_pid_containers=1,
        num_mpc_containers=1,
        num_files_per_mpc_container=40,
        mpc_compute_concurrency=4,
        status_updates=[],
    )
    common: CommonProductConfig = CommonProductConfig(
        input_path="https://bucket.s3.us-west-2.amazonaws.com/lift/partner/partner_e2e_input.csv",
        output_dir="https://bucket.s3.us-west-2.amazonaws.com/lift/partner",
        hmac_key="",
        padding_size=25,
    )
    product_config: ProductConfig = LiftConfig(
        common=common,
        k_anonymity_threshold=100,
        breakdown_key=BreakdownKey.get_default_key(),
    )

    return PrivateComputationInstance(
        infra_config=infra_config,
        product_config=product_config,
    )


def main() -> None:
    for path, instance in zip(
        (STAGE_STATE_PATH, LIFT_PC_PATH),
        (
            gen_dummy_stage_state_instance(),
            gen_dummy_pc_instance(),
        ),
    ):
        json_output = instance.dumps_schema()
        if path == STAGE_STATE_PATH:
            instance_dict = json.loads(json_output)
            instance_dict["invalid_parameter_to_exclude"] = (
                "This instance value should be excluded."
            )
            json_output = json.dumps(instance_dict)
        elif path == LIFT_PC_PATH:
            instance_dict = json.loads(json_output)
            instance_dict["infra_config"][
                "invalid_parameter_to_exclude"
            ] = "This instance value should be excluded."
            instance_dict["infra_config"]["instances"][0][
                "invalid_parameter_to_exclude"
            ] = "This instance value should be excluded."
            instance_dict["infra_config"]["instances"][1][
                "invalid_parameter_to_exclude"
            ] = "This instance value should be excluded."
            json_output = json.dumps(instance_dict)

        with open(path, "w") as f:
            f.write(json_output)


if __name__ == "__main__":
    main()
