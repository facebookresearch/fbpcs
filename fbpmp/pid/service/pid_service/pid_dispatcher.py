#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import pathlib
from typing import Any, DefaultDict, Dict, List, Optional, Tuple, Union

import networkx as nx
from fbpcs.service.onedocker import OneDockerService
from fbpcs.service.storage import StorageService
from fbpcs.util import yaml
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.pid.entity.pid_instance import (
    PIDInstanceStatus,
    PIDProtocol,
    PIDRole,
    PIDStageStatus,
)
from fbpmp.pid.entity.pid_stages import PIDStageFailureError, UnionPIDStage
from fbpmp.pid.repository.pid_instance import PIDInstanceRepository
from fbpmp.pid.service.pid_service import pid_execution_map
from fbpmp.pid.service.pid_service.dispatcher import Dispatcher
from fbpmp.pid.service.pid_service.pid_stage import PIDStage
from fbpmp.pid.service.pid_service.pid_stage_mapper import PIDStageMapper


# Builds the complete DAG and invoke stages as needed
class PIDDispatcher(Dispatcher):
    def __init__(self, instance_id: str, instance_repository: PIDInstanceRepository):
        self.instance_id = instance_id
        self.instance_repository = instance_repository
        self.dag = nx.DiGraph()  # build DAG of stages
        self.stage_inputs = {}  # keeps a track of the input to run each stage with
        self.logger: logging.Logger = logging.getLogger(__name__)

    def build_stages(
        self,
        input_path: str,
        output_path: str,
        num_shards: int,
        pid_config: Union[Dict[str, Any], str],
        protocol: PIDProtocol,
        role: PIDRole,
        onedocker_svc: OneDockerService,
        storage_svc: StorageService,
        onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
        fail_fast: bool,
        is_validating: Optional[bool] = False,
        synthetic_shard_path: Optional[str] = None,
        server_ips: Optional[List[str]] = None,
        data_path: Optional[str] = None,
        spine_path: Optional[str] = None,
        hmac_key: Optional[str] = None,
    ) -> None:
        flow_map = pid_execution_map.get_execution_flow(role, protocol)
        # read config into a dict if it was given as a path
        if isinstance(pid_config, str):
            config_dict = yaml.load(pathlib.Path(pid_config))
        else:
            config_dict = pid_config

        # maintain a map of enums to actual pid execution stages
        enum_to_stage_map = {}
        for node in flow_map.flow:
            stage = PIDStageMapper.get_stage(
                stage=node,
                config=config_dict,
                instance_repository=self.instance_repository,
                storage_svc=storage_svc,
                onedocker_svc=onedocker_svc,
                onedocker_binary_config_map=onedocker_binary_config_map,
                server_ips=server_ips,
            )
            enum_to_stage_map[node] = stage
            self.dag.add_node(stage)

            output_path_instance = output_path
            # Use data_path optional parameter as output path if the stage is shard
            if (
                stage.stage_type is UnionPIDStage.ADV_SHARD
                or stage.stage_type is UnionPIDStage.PUBLISHER_SHARD
            ) and data_path:
                output_path_instance = data_path

            # Use spine_path optional parameter as output path if the stage is protocol run
            if (
                stage.stage_type is UnionPIDStage.ADV_RUN_PID
                or stage.stage_type is UnionPIDStage.PUBLISHER_RUN_PID
            ) and spine_path:
                output_path_instance = spine_path

            # populate the stage_inputs map to be used from the run_all function
            self.stage_inputs[stage] = PIDStageMapper.get_input_for_stage(
                node,
                input_path,
                output_path_instance,
                num_shards,
                self.instance_id,
                fail_fast,
                is_validating,
                synthetic_shard_path,
                hmac_key,
            )

        # go over the flow map and create the connections among the interdependent stages
        for node in flow_map.flow:
            connections = flow_map.flow[node]
            for connection in connections:
                self.dag.add_edge(
                    enum_to_stage_map[node], enum_to_stage_map[connection]
                )

        # find out the beginning stages and add their input paths
        for stage_node in self._find_eligible_stages():
            self.stage_inputs[stage_node].add_to_inputs(input_path)

    async def run_all(
        self,
    ) -> None:
        ready_stages = self._find_eligible_stages()

        # Right before running any stages, write STARTED instance status to repo
        instance = self.instance_repository.read(self.instance_id)
        instance.status = PIDInstanceStatus.STARTED
        self.instance_repository.update(instance)

        iteration = 1
        while ready_stages:
            self.logger.info(f"Iteration {iteration} running stages {ready_stages}")
            iteration += 1

            # run all the eligible stages
            res = await self._run_eligible_stages(ready_stages)
            # validate if anything failed, if it did break
            # TODO: add retry/smart failover logic in future
            for stage, val in zip(ready_stages, res):
                # throw exception if not succeeded
                if val is not PIDStageStatus.COMPLETED:
                    raise PIDStageFailureError(f"Stage failed: {stage}")

            # clean up the complete stages
            self._cleanup_complete_stages(ready_stages)

            # get ready for the next set of eligible stages
            ready_stages = self._find_eligible_stages()
        self.logger.info("Finished all stages in PIDDispatcher")

        # Once successfully executed all stages, write COMPLETED instance status to repo
        instance = self.instance_repository.read(self.instance_id)
        instance.status = PIDInstanceStatus.COMPLETED
        self.instance_repository.update(instance)

    def _find_eligible_stages(self) -> List[PIDStage]:
        # Create a queue and find out which are the stages
        # which don't depend on any other stages
        # These would be the very first ones to be executed
        run_ready_stages = []
        for node in self.dag.nodes:
            if self.dag.in_degree(node) == 0:
                run_ready_stages.append(node)
        return run_ready_stages

    async def _run_eligible_stages(
        self, stages: List[PIDStage]
    ) -> Tuple[PIDStageStatus]:
        return await asyncio.gather(
            *[stage.run(self.stage_inputs[stage]) for stage in stages]
        )

    def _cleanup_complete_stages(self, stages: List[PIDStage]) -> None:
        # For all the done stages, update the next ones.
        # For successors, the input paths would depend on the
        # output paths of the currently finished stages
        for stage in stages:
            for next_stage in self.dag.successors(stage):
                for output_path in self.stage_inputs[stage].output_paths:
                    self.stage_inputs[next_stage].add_to_inputs(output_path)
            self.dag.remove_node(stage)
