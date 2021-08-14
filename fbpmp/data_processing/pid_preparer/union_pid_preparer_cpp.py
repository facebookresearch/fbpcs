#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
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
from typing import Optional

from fbpcp.entity.container_instance import ContainerInstanceStatus
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import PathType, StorageService
from fbpmp.data_processing.pid_preparer.preparer import UnionPIDDataPreparerService
from fbpmp.onedocker_binary_names import OneDockerBinaryNames


CPP_UNION_PID_PREPARER_PATH = pathlib.Path(
    os.environ.get("CPP_UNION_PID_PREPARER_PATH", "cpp_bin/union_pid_data_preparer")
)

DEFAULT_MAX_RETRY = 0

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
        tmp_directory: str = "/tmp/",
        max_retry: int = DEFAULT_MAX_RETRY,
        container_timeout: Optional[int] = None,
    ) -> None:
        asyncio.run(
            self.prepare_on_container_async(
                input_path,
                output_path,
                onedocker_svc,
                binary_version,
                tmp_directory,
                max_retry,
                container_timeout,
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
        max_retry: int = DEFAULT_MAX_RETRY,
        container_timeout: Optional[int] = None,
    ) -> None:
        logger = logging.getLogger(__name__)
        timeout = container_timeout or DEFAULT_CONTAINER_TIMEOUT_IN_SEC
        # TODO: Probably put exe in an env variable?
        # Try to align with existing paths
        cmd_args = " ".join(
            [
                f"--input_path={input_path}",
                f"--output_path={output_path}",
                f"--tmp_directory={tmp_directory}",
            ]
        )

        current_retry = 0
        status = ContainerInstanceStatus.UNKNOWN

        while status is not ContainerInstanceStatus.COMPLETED:
            # Retry for up to max_retry times on FAILED status
            if status is ContainerInstanceStatus.FAILED:
                current_retry += 1
                if current_retry > max_retry:
                    logger.info("Retry attempts exhausted.")
                    break
                logger.info(f"Retry attempt ({current_retry}/{max_retry})")

            exe = OneDockerBinaryNames.UNION_PID_PREPARER.value
            logger.info(f"Starting container: <{onedocker_svc.task_definition}, {exe} {cmd_args}>")
            # TODO: The ContainerService API for async instance creation only
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

            # Busy wait until the container is finished
            status = ContainerInstanceStatus.UNKNOWN
            logger.info("Task started, waiting for completion")
            while status not in [
                ContainerInstanceStatus.FAILED,
                ContainerInstanceStatus.COMPLETED,
            ]:
                container = onedocker_svc.get_containers([container.instance_id])[0]
                status = container.status
                # Sleep 5 seconds between calls to avoid an unintentional DDoS
                logger.debug(f"Latest status: {status}")
                await asyncio.sleep(5)
            logger.info(f"Process finished with status: {status}")
