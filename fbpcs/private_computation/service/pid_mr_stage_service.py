#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from typing import List, Optional

from fbpcp.util.s3path import S3Path

from fbpcs.common.entity.stage_state_instance import StageStateInstance
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.service.workflow import WorkflowService, WorkflowStatus

PID_WORKFLOW_CONFIGS = "workflowConfigs"
PID_RUN_CONFIGS = "runConfigs"
PIDMR = "pid_mr"
INTPUT = "inputPath"
OUTPUT = "outputPath"
INSTANCE = "instanceId"
RUNID = "run_id"
S3URIFORMAT = "s3://{bucket}/{key}"
PUB_PREFIX = "publisher_"
PARTNER_PREFIX = "partner_"


class PIDMRStageService(PrivateComputationStageService):
    """Handles business logic for the PID Mapreduce match stage."""

    def __init__(self, workflow_svc: WorkflowService) -> None:
        self.workflow_svc = workflow_svc

    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """This function run mr workflow service

        Args:
            pc_instance: the private computation instance to run mr match
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance
        """
        logging.info("PID-MR: Starting MR PID workflow")
        stage_state = StageStateInstance(
            pc_instance.infra_config.instance_id,
            pc_instance.current_stage.name,
        )
        pid_configs = pc_instance.product_config.common.pid_configs
        logging.info("PID-MR: pid_configs, %s", pid_configs)
        if (
            pid_configs
            and PIDMR in pid_configs
            and PID_RUN_CONFIGS in pid_configs[PIDMR]
            and PID_WORKFLOW_CONFIGS in pid_configs[PIDMR]
        ):
            logging.info("PID-MR: All configurations are available")
            pc_configs = {
                "numPidContainers": pc_instance.infra_config.num_pid_containers
            }
            data_configs = {
                INTPUT: self.get_s3uri_from_url(
                    pc_instance.product_config.common.input_path
                ),
                OUTPUT: self.get_s3uri_from_url(
                    pc_instance.pid_mr_stage_output_data_path
                ),
                INSTANCE: self.removePrefixForInstance(
                    pc_instance.infra_config.instance_id
                ),
                RUNID: pc_instance.infra_config.run_id,
            }
            pid_overall_configs = {
                **pid_configs[PIDMR][PID_RUN_CONFIGS],
                **data_configs,
                **pc_configs,
            }

            stage_state.instance_id = self.workflow_svc.start_workflow(
                pid_configs[PIDMR][PID_WORKFLOW_CONFIGS],
                pc_instance.infra_config.instance_id,
                pid_overall_configs,
            )
        else:
            raise ValueError("Missing relevant workflow configurations")
        pc_instance.infra_config.instances.append(stage_state)
        return pc_instance

    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Gets latest PrivateComputationInstance status

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        status = pc_instance.infra_config.status
        stage_instance = pc_instance.get_stage_instance()
        if stage_instance is not None:
            stage_id = stage_instance.instance_id
            pid_configs = pc_instance.product_config.common.pid_configs
            stage_state_instance_status = WorkflowStatus.STARTED
            if pid_configs:
                stage_state_instance_status = self.workflow_svc.get_workflow_status(
                    pid_configs[PIDMR][PID_WORKFLOW_CONFIGS], stage_id
                )
            current_stage = pc_instance.current_stage
            if stage_state_instance_status in [
                WorkflowStatus.STARTED,
                WorkflowStatus.CREATED,
                WorkflowStatus.UNKNOWN,
            ]:
                status = current_stage.started_status
            elif stage_state_instance_status is WorkflowStatus.COMPLETED:
                status = current_stage.completed_status
            elif stage_state_instance_status is WorkflowStatus.FAILED:
                status = current_stage.failed_status
            else:
                raise ValueError("Unknow stage status")

        return status

    def get_s3uri_from_url(self, path: str) -> str:
        s3Path = S3Path(path)
        return S3URIFORMAT.format(bucket=s3Path.bucket, key=s3Path.key)

    # In experimentation platform we introduce prefixes for instance_id. MR requires a common instance_id for publisher and partner.
    # Removing the prefixes temporarily. We will revisit this during production deployment.
    def removePrefixForInstance(self, instance_id: str) -> str:
        instance_id = instance_id[
            instance_id.startswith(PUB_PREFIX) and len(PUB_PREFIX) :
        ]
        instance_id = instance_id[
            instance_id.startswith(PARTNER_PREFIX) and len(PARTNER_PREFIX) :
        ]
        return instance_id
