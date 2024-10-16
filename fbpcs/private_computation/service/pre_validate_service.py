#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
from typing import Any, Dict, List

# pyre-fixme[21]: Could not find module `fbpcp.entity.container_instance`.
from fbpcp.entity.container_instance import ContainerInstanceStatus

# pyre-fixme[21]: Could not find module `fbpcp.entity.container_type`.
from fbpcp.entity.container_type import ContainerType

# pyre-fixme[21]: Could not find module `fbpcs.onedocker_binary_names`.
from fbpcs.onedocker_binary_names import OneDockerBinaryNames

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.entity.private_computation_instance`.
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.service.pc_pre_validation_stage_service`.
from fbpcs.private_computation.service.pc_pre_validation_stage_service import (
    PRE_VALIDATION_CHECKS_TIMEOUT,
)

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.service.pre_validation_util`.
from fbpcs.private_computation.service.pre_validation_util import get_cmd_args

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.service.private_computation`.
from fbpcs.private_computation.service.private_computation import (
    PrivateComputationService,
)

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.service.run_binary_base_service`.
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)

# pyre-fixme[21]: Could not find module `fbpcs.private_computation.service.utils`.
from fbpcs.private_computation.service.utils import generate_env_vars_dict

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation_cli.private_computation_service_wrapper`.
from fbpcs.private_computation_cli.private_computation_service_wrapper import (
    build_private_computation_service,
)


class PreValidateService:
    @staticmethod
    async def run_pre_validate_async(
        # pyre-fixme[11]: Annotation `PrivateComputationService` is not defined as a
        #  type.
        pc_service: PrivateComputationService,
        input_paths: List[str],
        logger: logging.Logger,
    ) -> None:
        region = pc_service.pc_validator_config.region
        onedocker_svc = pc_service.onedocker_svc
        # pyre-fixme[16]: Module `fbpcs` has no attribute `onedocker_binary_names`.
        binary_name = OneDockerBinaryNames.PC_PRE_VALIDATION.value
        binary_config = pc_service.onedocker_binary_config_map[binary_name]
        # pyre-fixme[16]: Module `service` has no attribute `utils`.
        env_vars = generate_env_vars_dict(repository_path=binary_config.repository_path)

        """
        [BE] T147526958 pass private computation role from cli.
        Since this service runs on partner PCE, we are hardcoding PrivateComputationRole to PARTNER in the code below.
        """
        cmd_args = [
            # pyre-fixme[16]: Module `service` has no attribute `pre_validation_util`.
            get_cmd_args(
                input_path=input_path,
                region=region,
                binary_config=binary_config,
                pre_validation_file_stream_flag=True,
                publisher_pc_pre_validation_flag=True,
                partner_pc_pre_validation_flag=True,
                # pyre-fixme[16]: Module `entity` has no attribute
                #  `private_computation_instance`.
                private_computation_role=PrivateComputationRole.PARTNER,
                input_path_start_ts=None,
                input_path_end_ts=None,
            )
            for input_path in input_paths
        ]

        # pyre-fixme[16]: Module `service` has no attribute `run_binary_base_service`.
        container_instances = await RunBinaryBaseService().start_containers(
            cmd_args,
            onedocker_svc,
            binary_config.binary_version,
            binary_name,
            # pyre-fixme[16]: Module `service` has no attribute
            #  `pc_pre_validation_stage_service`.
            timeout=PRE_VALIDATION_CHECKS_TIMEOUT,
            env_vars=env_vars,
            container_type=ContainerType.LARGE,
        )
        logger.info("Started container instances")

        # pyre-fixme[16]: Module `service` has no attribute `run_binary_base_service`.
        completed_containers = await RunBinaryBaseService().wait_for_containers_async(
            onedocker_svc,
            container_instances,
        )

        error_messages = []
        cluster = onedocker_svc.get_cluster()

        for container in completed_containers:
            if container.status != ContainerInstanceStatus.COMPLETED:
                task_id = container.instance_id.split("/")[-1]
                failed_task_link = f"https://{region}.console.aws.amazon.com/ecs/home?region={region}#/clusters/{cluster}/tasks/{task_id}/details"

                error_message = (
                    f"[PreValidate] - failed because of some failed validations. Please check the logs in ECS for task id '{task_id}' to see the validation issues:\n"
                    + f"Failed task link: {failed_task_link}"
                )
                error_messages.append(error_message)

        if error_messages:
            num_failed = len(error_messages)
            num_success = len(completed_containers) - num_failed
            failed = f"Number of containers that failed validation: {num_failed}\n"
            succeeded = f"Number of containers that passed validation: {num_success}\n"
            error_messages_string = "\n".join(error_messages)

            error_message = (
                f"ERROR - {failed}{succeeded}Errors: {error_messages_string}"
            )
            logger.error(error_message)
            raise Exception(error_message)

        logger.info(
            "SUCCESS - All validation containers returned success.\n"
            + f"Container count: {len(completed_containers)}\n"
            + f"Input paths: {input_paths}"
        )

    @staticmethod
    def pre_validate(
        config: Dict[str, Any],
        input_paths: List[str],
        logger: logging.Logger,
    ) -> None:
        # pyre-fixme[16]: Module `fbpcs` has no attribute `private_computation_cli`.
        pc_service = build_private_computation_service(
            config["private_computation"],
            config["mpc"],
            config["pid"],
            config.get("post_processing_handlers", {}),
            config.get("pid_post_processing_handlers", {}),
        )
        paths_string = "\n".join(input_paths)

        logger.info(f"Starting pre_validate on input_paths: {paths_string}")
        asyncio.run(
            PreValidateService.run_pre_validate_async(pc_service, input_paths, logger)
        )
