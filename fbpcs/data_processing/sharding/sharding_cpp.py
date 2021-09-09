#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import logging
import os
import pathlib
import subprocess
import sys
import tempfile
from typing import Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus, ContainerInstance
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import PathType, StorageService
from fbpcs.common.util.wait_for_containers import wait_for_containers_async
from fbpcs.data_processing.sharding.sharding import ShardingService, ShardType
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.pid.service.pid_service.pid_stage import PIDStage


CPP_SHARDER_PATH = pathlib.Path(os.environ.get("CPP_SHARDER_PATH", os.getcwd()))
CPP_SHARDER_HASHED_FOR_PID_PATH = pathlib.Path(
    os.environ.get("CPP_SHARDER_HASHED_FOR_PID_PATH", "cpp_bin/sharder_hashed_for_pid")
)

# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


class CppShardingService(ShardingService):
    def shard(
        self,
        shard_type: ShardType,
        filepath: str,
        output_base_path: str,
        file_start_index: int,
        num_output_files: int,
        storage_svc: Optional[StorageService] = None,
        hmac_key: Optional[str] = None,
    ) -> None:
        logger = logging.getLogger(__name__)
        local_path = filepath
        # If the path isn't local, assume the passed storage_svc can handle it
        if storage_svc and StorageService.path_type(filepath) != PathType.Local:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                local_path = f.name
            storage_svc.copy(filepath, local_path)

        local_output_base_path = output_base_path
        if (
            storage_svc
            and StorageService.path_type(
                PIDStage.get_sharded_filepath(output_base_path, file_start_index)
            )
            != PathType.Local
        ):
            with tempfile.NamedTemporaryFile(delete=False) as f:
                local_output_base_path = f.name

        if shard_type == ShardType.ROUND_ROBIN:
            exe_path = CPP_SHARDER_PATH
        elif shard_type == ShardType.HASHED_FOR_PID:
            exe_path = CPP_SHARDER_HASHED_FOR_PID_PATH
        else:
            raise TypeError(
                f"Unsupported shard type for CppShardingService: {shard_type}"
            )

        cmd = [
            f"{exe_path.absolute()}",
            f"--input_filename={local_path}",
            f"--output_base_path={local_output_base_path}",
            f"--file_start_index={file_start_index}",
            f"--num_output_files={num_output_files}",
        ]

        if hmac_key:
            cmd.append(f" --hmac_base64_key={hmac_key}")

        try:
            logger.info("Starting new process for C++ Sharder")
            logger.info(f"Running command: {cmd}")
            operating_dir = pathlib.Path(os.getcwd())
            proc = subprocess.Popen(
                cmd, cwd=operating_dir, stdout=subprocess.PIPE, stderr=sys.stderr
            )
            out, err = proc.communicate()
        except Exception as e:
            logger.warning("Encountered error while calling C++ sharder")
            raise e

        # Remember to copy the file back to the real output path
        if storage_svc and local_output_base_path != output_base_path:
            for i in range(num_output_files):
                storage_svc.copy(
                    PIDStage.get_sharded_filepath(local_output_base_path, i),
                    PIDStage.get_sharded_filepath(output_base_path, i),
                )

        logger.info(f"C++ Sharder returned status code {proc.returncode}")
        if proc.returncode != 0:
            logger.warning(f"C++ sharder returned nonzero status [{proc.returncode}]")
            raise Exception(f"{cmd} failed with return code {proc.returncode}")

    async def shard_async(
        self,
        shard_type: ShardType,
        filepath: str,
        output_base_path: str,
        file_start_index: int,
        num_output_files: int,
        storage_svc: Optional[StorageService] = None,
        hmac_key: Optional[str] = None,
    ) -> None:
        return self.shard(
            shard_type=shard_type,
            filepath=filepath,
            output_base_path=output_base_path,
            file_start_index=file_start_index,
            num_output_files=num_output_files,
            storage_svc=storage_svc,
            hmac_key=hmac_key,
        )

    def shard_on_container(
        self,
        shard_type: ShardType,
        filepath: str,
        output_base_path: str,
        file_start_index: int,
        num_output_files: int,
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        hmac_key: Optional[str] = None,
        container_timeout: Optional[int] = None,
        wait_for_containers: bool = True,
    ) -> ContainerInstance:
        return asyncio.run(
            self.shard_on_container_async(
                shard_type,
                filepath,
                output_base_path,
                file_start_index,
                num_output_files,
                onedocker_svc,
                binary_version,
                tmp_directory,
                hmac_key,
                container_timeout,
                wait_for_containers,
            )
        )

    async def shard_on_container_async(
        self,
        shard_type: ShardType,
        filepath: str,
        output_base_path: str,
        file_start_index: int,
        num_output_files: int,
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        hmac_key: Optional[str] = None,
        container_timeout: Optional[int] = None,
        wait_for_containers: bool = True,
    ) -> ContainerInstance:
        logger = logging.getLogger(__name__)
        timeout = container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC
        # TODO: Probably put exe in an env variable?
        # Try to align with existing paths
        exe = ""
        if shard_type == ShardType.ROUND_ROBIN:
            exe = OneDockerBinaryNames.SHARDER.value
        elif shard_type == ShardType.HASHED_FOR_PID:
            exe = OneDockerBinaryNames.SHARDER_HASHED_FOR_PID.value

        cmd_args = " ".join(
            [
                f"--input_filename={filepath}",
                f"--output_base_path={output_base_path}",
                f"--file_start_index={file_start_index}",
                f"--num_output_files={num_output_files}",
                f"--tmp_directory={tmp_directory}",
            ]
        )

        if hmac_key:
            cmd_args += f" --hmac_base64_key={hmac_key}"

        cmd = f"{exe} {cmd_args}"
        logger.info(f"Starting container: <{onedocker_svc.task_definition}, {cmd}>")

        # TODO: The OneDockerService API for async instance creation only
        # applies to a list of cmds, so we have to immediately dereference
        # to take the first element
        container = (
            await onedocker_svc.start_containers_async(
                package_name=exe,
                version=binary_version,
                cmd_args_list=[cmd_args],
                timeout=timeout,
            )
        )[0]

        logger.info("Task started")
        if wait_for_containers:
            # Busy wait until the container is finished
            # we're only passing one container, so we index with [0] again, as was done above
            container = (await wait_for_containers_async(onedocker_svc, [container]))[0]
            if container.status is ContainerInstanceStatus.FAILED:
                raise RuntimeError(f"Container {container.instance_id} failed.")
        return container
