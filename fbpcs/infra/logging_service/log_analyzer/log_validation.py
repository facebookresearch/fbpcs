#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import re
from re import Pattern

from fbpcs.infra.logging_service.log_analyzer.entity.flow_stage import FlowStage
from fbpcs.infra.logging_service.log_analyzer.entity.instance_flow import InstanceFlow
from fbpcs.infra.logging_service.log_analyzer.entity.log_context import LogContext
from fbpcs.infra.logging_service.log_analyzer.entity.run_study import RunStudy
from fbpcs.pl_coordinator.constants import MAX_NUM_INSTANCES, MIN_NUM_INSTANCES

# Minimum number of log lines expected
MIN_LOG_LINE_COUNT = 50
MIN_NUM_STAGES = 2
MIN_NUM_CONTAINERS = 1


class LogValidation:
    def __init__(
        self,
        logger: logging.Logger,
    ) -> None:
        self.logger = logger
        # container_id is like:
        # arn:aws:ecs:us-west-2:592513842793:task/onedocker-cluster-pc-e2e-test/6c2f2c23eace439b9631cbcc99363aa0
        self.re_container_id: Pattern[str] = re.compile(
            r"^arn:aws:ecs:[^:]+:\d+:[^:]+/[a-z\d]{32}$"
        )
        # log_url is like:
        # https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Fecs$252Fonedocker-container-pc-e2e-test/log-events/ecs$252Fonedocker-container-pc-e2e-test$252F6c2f2c23eace439b9631cbcc99363aa0
        self.re_container_log_url: Pattern[str] = re.compile(
            r"^https://.+/cloudwatch/home.+\$252F[a-z\d]{32}$"
        )

    def validate_one_runner_logs(
        self,
        run_study: RunStudy,
    ) -> None:
        self.logger.info("Validating summary output ...")
        assert run_study
        assert run_study.total_line_num >= MIN_LOG_LINE_COUNT
        assert run_study.first_log
        assert run_study.start_epoch_time
        assert run_study.summary_instances
        instance_count = len(run_study.summary_instances)
        assert instance_count >= MIN_NUM_INSTANCES
        assert instance_count <= MAX_NUM_INSTANCES
        assert run_study.error_line_count >= 0
        assert run_study.error_lines is not None
        assert run_study.instances
        assert len(run_study.instances) == instance_count
        # Validate the summary of the instance
        for inst in range(instance_count):
            instance_id = list(run_study.instances.keys())[inst]
            instance = run_study.instances[instance_id]
            assert instance.instance_id == instance_id
            self.validate_log_instance(instance, run_study)
            assert run_study.summary_instances[inst]

    def validate_log_instance(
        self,
        instance: InstanceFlow,
        run_study: RunStudy,
    ) -> None:
        self.validate_log_context(instance.context)
        assert instance.objective_id
        assert instance.cell_id
        assert run_study.summary_instances[0].startswith(
            f"i={instance.instance_id}/o={instance.objective_id}/c={instance.cell_id}"
        )
        assert instance.instance_failed_container_count <= 0
        assert instance.instance_error_line_count >= 0
        assert instance.instance_error_lines is not None
        if not instance.existing_instance_status:
            assert instance.instance_container_count >= MIN_NUM_CONTAINERS
            assert instance.summary_stages
            assert len(instance.summary_stages) >= MIN_NUM_STAGES
            assert instance.stages
            assert len(instance.summary_stages) == len(instance.stages)
            # Validate the summary of the stages
        count_stage_with_container = 0
        for stage in instance.stages:
            self.validate_log_stage(stage)
            count_stage_with_container += 1 if stage.container_count else 0
        if not instance.existing_instance_status:
            assert count_stage_with_container >= MIN_NUM_CONTAINERS

    def validate_log_stage(
        self,
        stage: FlowStage,
    ) -> None:
        self.validate_log_context(stage.context)
        assert stage.stage_id
        assert stage.failed_container_count <= 0
        assert stage.container_count == len(stage.containers)
        # Validate the summary of containers
        for container in stage.containers:
            self.validate_log_context(container.context)
            assert container.container_id
            assert self.re_container_id.search(container.container_id)
            assert container.log_url
            assert self.re_container_log_url.search(container.log_url)
            assert container.status

    def validate_log_context(
        self,
        context: LogContext,
    ) -> None:
        assert context.line_num > 0
        assert float(str(context.elapsed_second))
        assert float(str(context.epoch_time))
        assert context.utc_time
