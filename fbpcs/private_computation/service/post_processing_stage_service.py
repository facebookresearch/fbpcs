#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from fbpcs.private_computation.entity.private_computation_instance import PrivateComputationInstanceStatus
import asyncio
import logging
from typing import Dict, List, Optional

from fbpcp.service.storage import StorageService
from fbpcs.post_processing_handler.post_processing_handler import (
    PostProcessingHandler,
    PostProcessingHandlerStatus,
)
from fbpcs.post_processing_handler.post_processing_instance import (
    PostProcessingInstance,
    PostProcessingInstanceStatus,
)
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)


class PostProcessingStageService(PrivateComputationStageService):
    """Handles business logic for the private computation post processing stage

    Private attributes:
        _storage_svc: Used to read/write files during private computation runs, e.g. read the final results from S3
        _post_processing_handlers: maps handler name to handler instance. Handler instance defines the business logic of the handler
        _aggregated_result_path: Optional path to private computation final results JSON file
    """

    def __init__(
        self,
        storage_svc: StorageService,
        post_processing_handlers: Dict[str, PostProcessingHandler],
        aggregated_result_path: Optional[str] = None,
    ) -> None:
        self._storage_svc = storage_svc
        self._post_processing_handlers = post_processing_handlers
        self._aggregated_result_path = aggregated_result_path
        self._logger: logging.Logger = logging.getLogger(__name__)

    # TODO T88759390: Make this function truly async. It is not because it calls blocking functions.
    # Make an async version of run_async() so that it can be called by Thrift
    async def run_async(
        self,
        pc_instance: PrivateComputationInstance,
        server_ips: Optional[List[str]] = None,
    ) -> PrivateComputationInstance:
        """Runs the private computation post processing handlers stage

        Post processing handlers are designed to run after final results are available. You can write
        post processing handlers to download results from cloud storage, send you an email, etc.

        Args:
            pc_instance: the private computation instance to run post processing handlers with
            server_ips: only used by the partner role. These are the ip addresses of the publisher's containers.

        Returns:
            An updated version of pc_instance that stores a post processing instance
        """

        post_processing_handlers_statuses = None
        if pc_instance.instances:
            last_instance = pc_instance.instances[-1]
            if (
                isinstance(last_instance, PostProcessingInstance)
                and last_instance.handler_statuses.keys()
                == self._post_processing_handlers.keys()
            ):
                self._logger.info("Copying statuses from last instance")
                post_processing_handlers_statuses = (
                    last_instance.handler_statuses.copy()
                )

        post_processing_instance = PostProcessingInstance.create_instance(
            instance_id=pc_instance.instance_id
            + "_post_processing"
            + str(pc_instance.retry_counter),
            handlers=self._post_processing_handlers,
            handler_statuses=post_processing_handlers_statuses,
            status=PostProcessingInstanceStatus.STARTED,
        )

        pc_instance.instances.append(post_processing_instance)

        # if any handlers fail, then the post_processing_instance status will be
        # set to failed, as will the pc_instance status
        await asyncio.gather(
            *[
                self._run_post_processing_handler(
                    pc_instance,
                    post_processing_instance,
                    name,
                    handler,
                )
                for name, handler in self._post_processing_handlers.items()
                if post_processing_instance.handler_statuses[name]
                != PostProcessingHandlerStatus.COMPLETED
            ]
        )

        # if any of the handlers failed, then the status of the post processing instance would have
        # been set to failed. If none of them failed, then tht means all of the handlers completed, so
        # we can set the status to completed.
        if post_processing_instance.status != PostProcessingInstanceStatus.FAILED:
            post_processing_instance.status = PostProcessingInstanceStatus.COMPLETED
            pc_instance.status = pc_instance.current_stage.completed_status
        return pc_instance


    async def _run_post_processing_handler(
        self,
        private_computation_instance: PrivateComputationInstance,
        post_processing_instance: PostProcessingInstance,
        handler_name: str,
        handler: PostProcessingHandler,
    ) -> None:
        self._logger.info(f"Starting post processing handler: {handler_name=}")
        post_processing_instance.handler_statuses[
            handler_name
        ] = PostProcessingHandlerStatus.STARTED
        try:
            await handler.run(self._storage_svc, private_computation_instance)
            self._logger.info(f"Completed post processing handler: {handler_name=}")
            post_processing_instance.handler_statuses[
                handler_name
            ] = PostProcessingHandlerStatus.COMPLETED
        except Exception as e:
            self._logger.exception(e)
            self._logger.error(f"Failed post processing handler: {handler_name=}")
            post_processing_instance.handler_statuses[
                handler_name
            ] = PostProcessingHandlerStatus.FAILED
            post_processing_instance.status = PostProcessingInstanceStatus.FAILED


    def get_status(
        self,
        pc_instance: PrivateComputationInstance,
    ) -> PrivateComputationInstanceStatus:
        """Updates the PostProcessingInstances and gets latest PrivateComputationInstance status

        Arguments:
            private_computation_instance: The PC instance that is being updated

        Returns:
            The latest status for private_computation_instance
        """
        status = pc_instance.status
        if pc_instance.instances:
            # Only need to update the last stage/instance
            last_instance = pc_instance.instances[-1]
            if not isinstance(last_instance, PostProcessingInstance):
                return status

            post_processing_instance_status = pc_instance.instances[-1].status

            stage = pc_instance.current_stage
            if post_processing_instance_status is PostProcessingInstanceStatus.STARTED:
                status = stage.started_status
            elif post_processing_instance_status is PostProcessingInstanceStatus.COMPLETED:
                status = stage.completed_status
            elif post_processing_instance_status is PostProcessingInstanceStatus.FAILED:
                status = stage.failed_status

        return status
