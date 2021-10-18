#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import multiprocessing
import os
import pathlib
import subprocess
import sys
import tempfile
from typing import List, Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import PathType, StorageService
from fbpcs.data_processing.lift_id_combiner.lift_id_spine_combiner_service import (
    LiftIdSpineCombinerService,
)
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.service.pid_service.pid_stage import PIDStage


CPP_COMBINER_PATH = pathlib.Path(
    os.environ.get("CPP_LIFT_ID_SPINE_COMBINER_PATH", "cpp_bin/lift_id_spine_combiner")
)

# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


class CppLiftIdSpineCombinerService(LiftIdSpineCombinerService):
    def combine_single(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        log_path: Optional[pathlib.Path] = None,
        log_level: int = logging.INFO,
        storage_svc: Optional[StorageService] = None,
    ) -> None:
        # First configure the root logger to ensure log messages are output
        if log_path is not None:
            logging.basicConfig(filename=log_path, level=log_level)
        else:
            logging.basicConfig(level=log_level)
        logger = logging.getLogger(__name__)

        # First check if we need to copy the files from the StorageService
        local_spine_path = spine_path
        local_data_path = data_path
        local_output_path = output_path
        # If the path isn't local, assume the passed storage_svc can handle it
        if storage_svc and StorageService.path_type(spine_path) != PathType.Local:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                local_spine_path = f.name
            storage_svc.copy(spine_path, local_spine_path)

        if storage_svc and StorageService.path_type(data_path) != PathType.Local:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                local_data_path = f.name
            storage_svc.copy(data_path, local_data_path)

        if storage_svc and StorageService.path_type(output_path) != PathType.Local:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                local_output_path = f.name

        cmd = [
            f"{CPP_COMBINER_PATH.absolute()}",
            f"--spine_path={local_spine_path}",
            f"--data_path={local_data_path}",
            f"--output_path={local_output_path}",
        ]

        try:
            logger.info("Starting new process for C++ Combiner")
            logger.info(f"Running command: {cmd}")
            operating_dir = pathlib.Path(os.getcwd())
            proc = subprocess.Popen(
                cmd, cwd=operating_dir, stdout=subprocess.PIPE, stderr=sys.stderr
            )
            out, err = proc.communicate()
        except Exception as e:
            logger.warning("Encountered error while calling C++ combiner")
            raise e

        # Remember to copy the file back to the real output path
        if storage_svc and StorageService.path_type(output_path) != PathType.Local:
            storage_svc.copy(local_output_path, output_path)

        logger.info(f"C++ Combiner returned status code {proc.returncode}")
        if proc.returncode != 0:
            logger.warning(f"C++ combiner returned nonzero status [{proc.returncode}]")
            raise Exception(f"{cmd} failed with return code {proc.returncode}")

    # Using multiprocessing.Pool may be better way to manage the
    # processes spawned in here. Look at attribution_id_spine_combiner.py
    # for reference
    def combine(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        log_path: Optional[pathlib.Path] = None,
        log_level: int = logging.INFO,
        storage_svc: Optional[StorageService] = None,
    ) -> None:
        logger = logging.getLogger(__name__)
        # TODO: Combiner could be made async so we don't have to spawn our
        # own ThreadPoolExecutor here and instead use async primitives
        procs = []
        for shard in range(num_shards):
            # TODO: There's a weird dependency between these two services
            # LiftIdSpineCombiner should operate independently of PIDStage
            next_spine_path = PIDStage.get_sharded_filepath(spine_path, shard)
            next_data_path = PIDStage.get_sharded_filepath(data_path, shard)
            next_output_path = PIDStage.get_sharded_filepath(output_path, shard)
            proc = multiprocessing.Process(
                target=self.combine_single,
                args=(next_spine_path, next_data_path, next_output_path),
                kwargs={"storage_svc": storage_svc},
            )
            procs.append(proc)
            proc.start()

        # Wait for all subprocesses to finish
        for i, proc in enumerate(procs):
            logger.debug(f"[{self}] Waiting for shard {i+1} / {len(procs)}")
            proc.join()

        for proc in procs:
            if proc.exitcode != 0:
                raise Exception(
                    f"{proc.name} exited with non-zero code {proc.exitcode}"
                )

    def _get_combine_cmd_for_container(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        tmp_directory: str,
    ) -> str:
        # TODO: Probably put exe in an env variable?
        # Try to align with existing paths
        exe = "lift_id_combiner"
        cmd_args = " ".join(
            [
                f"--spine_path={spine_path}",
                f"--data_path={data_path}",
                f"--output_path={output_path}",
                f"--tmp_directory={tmp_directory}",
            ]
        )
        return f"{exe} {cmd_args}"

    def combine_on_container(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        container_timeout: Optional[int] = None,
    ) -> None:
        asyncio.run(
            self.combine_on_container_async(
                spine_path,
                data_path,
                output_path,
                num_shards,
                onedocker_svc,
                binary_version,
                tmp_directory,
                container_timeout,
            )
        )

    async def combine_on_container_async(
        self,
        spine_path: str,
        data_path: str,
        output_path: str,
        num_shards: int,
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        container_timeout: Optional[int] = None,
    ) -> None:
        logger = logging.getLogger(__name__)
        timeout = container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC
        # TODO: Combiner could be made async so we don't have to spawn our
        # own ThreadPoolExecutor here and instead use async primitives
        cmds: List[str] = []
        for shard in range(num_shards):
            # TODO: There's a weird dependency between these two services
            # LiftIdSpineCombiner should operate independently of PIDStage
            next_spine_path = PIDStage.get_sharded_filepath(spine_path, shard)
            next_data_path = PIDStage.get_sharded_filepath(data_path, shard)
            next_output_path = PIDStage.get_sharded_filepath(output_path, shard)
            cmd = self._get_combine_cmd_for_container(
                next_spine_path, next_data_path, next_output_path, tmp_directory
            )
            cmds.append(cmd)

        containers = await onedocker_svc.start_containers_async(
            package_name=OneDockerBinaryNames.LIFT_ID_SPINE_COMBINER.value,
            version=binary_version,
            cmd_args_list=cmds,
            timeout=timeout,
        )

        # Busy wait until all containers are finished
        any_failed = False
        for shard, container in enumerate(containers):
            # Busy wait until the container is finished
            status = ContainerInstanceStatus.UNKNOWN
            logger.info(f"Task[{shard}] started, waiting for completion")
            while status not in [
                ContainerInstanceStatus.FAILED,
                ContainerInstanceStatus.COMPLETED,
            ]:
                container = onedocker_svc.get_containers([container.instance_id])[0]
                status = container.status
                # Sleep 5 seconds between calls to avoid an unintentional DDoS
                logger.debug(f"Latest status: {status}")
                await asyncio.sleep(5)
            logger.info(
                f"container_id({container.instance_id}) finished with status: {status}"
            )
            if status is not ContainerInstanceStatus.COMPLETED:
                logger.error(f"Container {container.instance_id} failed!")
                any_failed = True
        if any_failed:
            raise RuntimeError(
                "One or more containers failed. See the logs above to find the exact container_id"
            )
