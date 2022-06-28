#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
import os
import pathlib
import subprocess
import sys
import tempfile
from typing import Dict, Optional

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus
from fbpcp.error.pcp import ThrottlingError
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import PathType, StorageService
from fbpcs.data_processing.pid_preparer.preparer import UnionPIDDataPreparerService
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.private_computation.service.retry_handler import RetryHandler
from fbpcs.private_computation.service.run_binary_base_service import (
    RunBinaryBaseService,
)


CPP_UNION_PID_PREPARER_PATH = pathlib.Path(
    os.environ.get("CPP_UNION_PID_PREPARER_PATH", "cpp_bin/union_pid_data_preparer")
)

# 10800 s = 3 hrs
DEFAULT_CONTAINER_TIMEOUT_IN_SEC = 10800


class CppUnionPIDDataPreparerService(UnionPIDDataPreparerService):
    def prepare(
        self,
        input_path: str,
        output_path: str,
        log_path: Optional[pathlib.Path] = None,
        log_level: int = logging.INFO,
        storage_svc: Optional[StorageService] = None,
    ) -> None:
        if log_path is not None:
            logging.basicConfig(filename=log_path, level=log_level)
        else:
            logging.basicConfig(level=log_level)
        logger = logging.getLogger(__name__)

        # First check if we need to copy the files from the StorageService
        local_inpath = input_path
        local_outpath = output_path
        # If the path isn't local, assume the passed storage_svc can handle it
        if storage_svc and StorageService.path_type(input_path) != PathType.Local:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                local_inpath = f.name
            storage_svc.copy(input_path, local_inpath)

        if storage_svc and StorageService.path_type(output_path) != PathType.Local:
            with tempfile.NamedTemporaryFile(delete=False) as f:
                local_outpath = f.name

        cmd = [
            f"{CPP_UNION_PID_PREPARER_PATH.absolute()}",
            f"--input_path={local_inpath}",
            f"--output_path={local_outpath}",
        ]

        try:
            logger.info("Starting new process for C++ Preparer")
            logger.info(f"Running command: {cmd}")
            operating_dir = pathlib.Path(os.getcwd())
            proc = subprocess.Popen(
                cmd, cwd=operating_dir, stdout=subprocess.PIPE, stderr=sys.stderr
            )
            out, err = proc.communicate()
        except Exception as e:
            logger.warning("Encountered error while calling C++ preparer")
            raise e

        # Remember to copy the file back to the real output path
        if storage_svc and StorageService.path_type(output_path) != PathType.Local:
            storage_svc.copy(local_outpath, output_path)

        logger.info(f"C++ Preparer returned status code {proc.returncode}")
        if proc.returncode != 0:
            logger.warning(f"C++ preparer returned nonzero status [{proc.returncode}]")
            raise Exception(f"{cmd} failed with return code {proc.returncode}")

    def prepare_on_container(
        self,
        input_path: str,
        output_path: str,
        onedocker_svc: OneDockerService,
        binary_version: str,
        max_column_count: int = 1,
        tmp_directory: str = "/tmp/",
        container_timeout: Optional[int] = None,
        wait_for_container: bool = True,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> ContainerInstance:
        return asyncio.run(
            self.prepare_on_container_async(
                input_path,
                output_path,
                onedocker_svc,
                binary_version,
                tmp_directory,
                max_column_count,
                container_timeout,
                wait_for_container,
                env_vars,
            )
        )

    async def prepare_on_container_async(
        self,
        input_path: str,
        output_path: str,
        # TODO: Support custom log path
        onedocker_svc: OneDockerService,
        binary_version: str,
        tmp_directory: str = "/tmp/",
        max_column_count: int = 1,
        container_timeout: Optional[int] = None,
        wait_for_container: bool = True,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> ContainerInstance:
        logger = logging.getLogger(__name__)
        timeout = container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC
        # TODO: Probably put exe in an env variable?
        # Try to align with existing paths
        cmd_args = " ".join(
            [
                f"--input_path={input_path}",
                f"--output_path={output_path}",
                f"--tmp_directory={tmp_directory}",
                f"--max_column_cnt={max_column_count}",
            ]
        )

        current_retry = 0
        status = ContainerInstanceStatus.UNKNOWN
        exe = OneDockerBinaryNames.UNION_PID_PREPARER.value
        container = None
        while status is not ContainerInstanceStatus.COMPLETED:
            logger.info(
                f"Starting container: <{onedocker_svc.task_definition}, {exe} {cmd_args}>"
            )
            # TODO: The ContainerService API for async instance creation only
            # applies to a list of cmds, so we have to immediately dereference
            # to take the first element
            pending_containers = onedocker_svc.start_containers(
                package_name=exe,
                version=binary_version,
                cmd_args_list=[cmd_args],
                timeout=timeout,
                env_vars=env_vars,
            )

            with RetryHandler(
                ThrottlingError, logger=logger, backoff_seconds=30
            ) as retry_handler:
                container = (
                    await retry_handler.execute(
                        onedocker_svc.wait_for_pending_containers,
                        [container.instance_id for container in pending_containers],
                    )
                )[0]

            # Busy wait until the container is finished
            if wait_for_container:
                container = (
                    await RunBinaryBaseService.wait_for_containers_async(
                        onedocker_svc, [container]
                    )
                )[0]
                status = container.status
            else:
                return container
        if container is None:
            raise RuntimeError(
                f"Failed to start any containers after {1 + current_retry} attempts"
            )
        logger.info(f"Process finished with status: {container.status}")
        return container
