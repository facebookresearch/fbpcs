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
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcp.util import yaml
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.pid.entity.pid_instance import (
    PIDInstance,
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

        instance = self.instance_repository.read(self.instance_id)
        # if this is the first run, initialize the stages_status dict. Knowing
        # all of the different possible stages will help the pid service determine when
        # an instance is done later on.
        if not instance.stages_status:
            instance.stages_status = {
                str(stage.stage_type): PIDStageStatus.UNKNOWN
                for stage in enum_to_stage_map.values()
            }
            self.instance_repository.update(instance)

        # find out the beginning stages and add their input paths
        for stage_node in self._find_eligible_stages():
            self.stage_inputs[stage_node].add_to_inputs(input_path)

    async def run_stage(
        self, stage: PIDStage, wait_for_containers: bool = True
    ) -> PIDStageStatus:
        if stage not in self._find_eligible_stages():
            raise PIDStageFailureError(f"{stage} is not yet eligible to be run.")
        instance = self.instance_repository.read(self.instance_id)
        if (
            instance.stages_status.get(str(stage.stage_type), None)
            is PIDStageStatus.STARTED
        ):
            raise PIDStageFailureError(f"{stage} already has status STARTED")

        res = await stage.run(
            self.stage_inputs[stage], wait_for_containers=wait_for_containers
        )
        self.logger.info(f"{stage}: {res}")
        if res is PIDStageStatus.FAILED:
            self._update_instance_status(PIDInstanceStatus.FAILED)
            raise PIDStageFailureError(f"Stage failed: {stage}")
        elif res is PIDStageStatus.COMPLETED:
            self._cleanup_complete_stages([stage])
        return res

    async def run_next(self) -> PIDInstanceStatus:
        ready_stages = self._find_eligible_stages()
        if not ready_stages:
            self._update_instance_status(PIDInstanceStatus.COMPLETED)
            return PIDInstanceStatus.COMPLETED

        self._update_instance_status(PIDInstanceStatus.STARTED)

        await asyncio.gather(*[self.run_stage(stage) for stage in ready_stages])
        instance = self.instance_repository.read(self.instance_id)
        return instance.status

    async def run_all(
        self,
    ) -> None:
        status = PIDInstanceStatus.STARTED
        while status is not PIDInstanceStatus.COMPLETED:
            status = await self.run_next()

        self.logger.info("Finished all stages in PIDDispatcher")

    def _find_eligible_stages(self) -> List[PIDStage]:
        # Create a queue and find out which are the stages
        # which don't depend on any other stages
        # These would be the very first ones to be executed

        # clear out the already finished stages
        self._cleanup_complete_stages()
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

    def _cleanup_complete_stages(
        self, finished_stages: Optional[List[PIDStage]] = None
    ) -> None:
        # For all the done stages, update the next ones.
        # For successors, the input paths would depend on the
        # output paths of the currently finished stages

        if not finished_stages:
            instance = self.instance_repository.read(self.instance_id)
            finished_stages = [
                node
                for node in self.dag.nodes
                if self.dag.in_degree(node) == 0
                and instance.stages_status.get(str(node.stage_type), None)
                is PIDStageStatus.COMPLETED
            ]
        for stage in finished_stages:
            for next_stage in self.dag.successors(stage):
                for output_path in self.stage_inputs[stage].output_paths:
                    self.stage_inputs[next_stage].add_to_inputs(output_path)
            self.dag.remove_node(stage)

    def _update_instance_status(self, new_status: PIDInstanceStatus) -> PIDInstance:
        instance = self.instance_repository.read(self.instance_id)
        if instance.status is not new_status:
            instance.status = new_status
            self.instance_repository.update(instance)
        return instance
