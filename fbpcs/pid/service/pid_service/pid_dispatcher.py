#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
from typing import DefaultDict, Dict, List, Optional, Tuple

import networkx as nx
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.pid.entity.pid_instance import (
    PIDInstance,
    PIDInstanceStatus,
    PIDProtocol,
    PIDRole,
    PIDStageStatus,
)
from fbpcs.pid.entity.pid_stages import PIDStageFailureError, UnionPIDStage
from fbpcs.pid.repository.pid_instance import PIDInstanceRepository
from fbpcs.pid.service.pid_service import pid_execution_map
from fbpcs.pid.service.pid_service.dispatcher import Dispatcher
from fbpcs.pid.service.pid_service.pid_stage import PIDStage
from fbpcs.pid.service.pid_service.pid_stage_mapper import PIDStageMapper


# Builds the complete DAG and invoke stages as needed
class PIDDispatcher(Dispatcher):
    def __init__(self, instance_id: str, instance_repository: PIDInstanceRepository):
        self.instance_id = instance_id
        self.instance_repository = instance_repository
        self.dag = nx.DiGraph()  # build DAG of stages
        self.stage_inputs = {}  # keeps a track of the input to run each stage with
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.enum_to_stage_map: Dict[UnionPIDStage, PIDStage] = {}

    def build_stages(
        self,
        input_path: str,
        output_path: str,
        num_shards: int,
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

        # maintain a map of enums to actual pid execution stages
        self.enum_to_stage_map = {}
        for node in flow_map.flow:
            stage = PIDStageMapper.get_stage(
                stage=node,
                instance_repository=self.instance_repository,
                storage_svc=storage_svc,
                onedocker_svc=onedocker_svc,
                onedocker_binary_config_map=onedocker_binary_config_map,
                server_ips=server_ips,
            )
            self.enum_to_stage_map[node] = stage
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

            if (
                stage.stage_type is UnionPIDStage.ADV_SHARD
                or stage.stage_type is UnionPIDStage.PUBLISHER_SHARD
            ):
                self.stage_inputs[stage].add_to_inputs(input_path)

        # go over the flow map and create the connections among the interdependent stages
        for node in flow_map.flow:
            connections = flow_map.flow[node]
            for connection in connections:
                self.dag.add_edge(
                    self.enum_to_stage_map[node], self.enum_to_stage_map[connection]
                )

        instance = self.instance_repository.read(self.instance_id)
        # if this is the first run, initialize the stages_status dict. Knowing
        # all of the different possible stages will help the pid service determine when
        # an instance is done later on.
        if not instance.stages_status:
            instance.stages_status = {
                stage.stage_type: PIDStageStatus.UNKNOWN
                for stage in self.enum_to_stage_map.values()
            }
            self.instance_repository.update(instance)

        # remove completed stages from the dag
        self._cleanup_complete_stages()

    async def run_stage(
        self,
        stage: PIDStage,
        wait_for_containers: bool = True,
        container_timeout: Optional[int] = None,
    ) -> PIDStageStatus:
        if stage not in self._find_eligible_stages():
            raise PIDStageFailureError(f"{stage} is not yet eligible to be run.")
        instance = self.instance_repository.read(self.instance_id)
        if instance.stages_status.get(stage.stage_type, None) is PIDStageStatus.STARTED:
            raise PIDStageFailureError(f"{stage} already has status STARTED")

        self._update_instance_status(PIDInstanceStatus.STARTED, stage)

        res = await stage.run(
            self.stage_inputs[stage],
            wait_for_containers=wait_for_containers,
            container_timeout=container_timeout,
        )
        self.logger.info(f"{stage}: {res}")
        if res is PIDStageStatus.FAILED:
            self._update_instance_status(PIDInstanceStatus.FAILED, stage)
            raise PIDStageFailureError(f"Stage failed: {stage}")
        elif res is PIDStageStatus.COMPLETED:
            self._cleanup_complete_stages([stage])
        return res

    async def run_next(self) -> bool:
        ready_stages = self._find_eligible_stages()
        if not ready_stages:
            self.logger.info("There are no eligible stages to run at this time")
            return False

        # if this is not the last stage (number of nodes != 1), wait for the containers
        # if this is the last stage (number of nodes == 1), then do not wait for containers
        await asyncio.gather(
            *[
                self.run_stage(stage, self.dag.number_of_nodes() != 1)
                for stage in ready_stages
            ]
        )
        return True

    async def run_all(
        self,
    ) -> None:
        res = True
        while res:
            res = await self.run_next()

        self.logger.info("All eligible stages in PIDDispatcher have been ran")

    def _find_eligible_stages(self) -> List[PIDStage]:
        # Create a queue and find out which are the stages
        # which don't depend on any other stages
        # These would be the very first ones to be executed

        # clear out the already finished stages
        self._cleanup_complete_stages()
        instance = self.instance_repository.read(self.instance_id)
        run_ready_stages = []
        for node in self.dag.nodes:
            # nodes with no dependencies left and who have not already been
            # started are eligible to run
            if (
                self.dag.in_degree(node) == 0
                and instance.stages_status.get(node.stage_type, None)
                is not PIDStageStatus.STARTED
            ):
                run_ready_stages.append(node)
        return run_ready_stages

    async def _run_eligible_stages(
        self, stages: List[PIDStage]
    ) -> Tuple[PIDStageStatus]:
        return await asyncio.gather(
            *[stage.run(self.stage_inputs[stage]) for stage in stages]
        )

    def get_pid_stage(
        self,
        pid_union_stage: Optional[UnionPIDStage],
    ) -> Optional[PIDStage]:
        """This function translate the UnionPIDStage into PIDStage by using
        the enum_to_stage_map dictionary.

        If the pid_union_stage arg is None, or we cannot find the corresponding PIDStage,
        this function will return None.
        """
        if pid_union_stage is not None:
            return self.enum_to_stage_map.get(pid_union_stage, None)
        else:
            return None

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
                if instance.stages_status.get(node.stage_type, None)
                is PIDStageStatus.COMPLETED
            ]
        for stage in finished_stages:
            for next_stage in self.dag.successors(stage):
                for output_path in self.stage_inputs[stage].output_paths:
                    self.stage_inputs[next_stage].add_to_inputs(output_path)
            self.dag.remove_node(stage)

    def _update_instance_status(
        self, new_status: PIDInstanceStatus, current_stage: PIDStage
    ) -> PIDInstance:
        instance = self.instance_repository.read(self.instance_id)
        if instance.status is not new_status:
            instance.status = new_status
            instance.current_stage = current_stage.stage_type
            self.instance_repository.update(instance)
        elif instance.current_stage is not current_stage.stage_type:
            instance.current_stage = current_stage.stage_type
            self.instance_repository.update(instance)
        return instance
