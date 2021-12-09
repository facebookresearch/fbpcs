#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import enum
import logging
import os
import pathlib
from typing import Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus, ContainerInstance
from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.util.wait_for_containers import wait_for_containers_async
from fbpcs.onedocker_binary_names import OneDockerBinaryNames


CPP_SHARDER_PATH = pathlib.Path(os.environ.get("CPP_SHARDER_PATH", os.getcwd()))
CPP_SHARDER_HASHED_FOR_PID_PATH = pathlib.Path(
    os.environ.get("CPP_SHARDER_HASHED_FOR_PID_PATH", "cpp_bin/sharder_hashed_for_pid")
)

# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800

class ShardType(enum.Enum):
    ROUND_ROBIN = 1
    HASHED_FOR_PID = 2


class ShardingService():
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
        pending_containers = onedocker_svc.start_containers(
            package_name=exe,
            version=binary_version,
            cmd_args_list=[cmd_args],
            timeout=timeout,
        )

        container = (
            await onedocker_svc.wait_for_pending_containers(
                [container.instance_id for container in pending_containers]
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
