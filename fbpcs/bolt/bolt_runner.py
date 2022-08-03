#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
from time import time
from typing import List, Optional, Tuple

from fbpcs.bolt.bolt_client import BoltClient

from fbpcs.bolt.bolt_job import BoltJob
from fbpcs.bolt.constants import (
    DEFAULT_MAX_PARALLEL_RUNS,
    DEFAULT_NUM_TRIES,
    INVALID_STATUS_LIST,
    RETRY_INTERVAL,
    WAIT_VALID_STATUS_TIMEOUT,
)
from fbpcs.bolt.exceptions import (
    IncompatibleStageError,
    NoServerIpsException,
    StageFailedException,
    StageTimeoutException,
    WaitValidStatusTimeout,
)
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)

from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.utils.logger_adapter import LoggerAdapter


class BoltRunner:
    def __init__(
        self,
        publisher_client: BoltClient,
        partner_client: BoltClient,
        max_parallel_runs: Optional[int] = None,
        num_tries: Optional[int] = None,
        skip_publisher_creation: Optional[bool] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.publisher_client = publisher_client
        self.partner_client = partner_client
        self.semaphore = asyncio.Semaphore(
            max_parallel_runs or DEFAULT_MAX_PARALLEL_RUNS
        )
        self.logger: logging.Logger = (
            logging.getLogger(__name__) if logger is None else logger
        )
        self.num_tries: int = num_tries or DEFAULT_NUM_TRIES
        self.skip_publisher_creation = skip_publisher_creation

    async def run_async(
        self,
        jobs: List[BoltJob],
    ) -> List[bool]:
        return list(await asyncio.gather(*[self.run_one(job=job) for job in jobs]))

    async def run_one(self, job: BoltJob) -> bool:
        async with self.semaphore:
            try:
                publisher_id, partner_id = await self._get_or_create_instances(job)
                logger = LoggerAdapter(logger=self.logger, prefix=partner_id)
                await self.wait_valid_publisher_status(
                    instance_id=publisher_id,
                    poll_interval=job.poll_interval,
                    timeout=WAIT_VALID_STATUS_TIMEOUT,
                )
                stage = await self.get_next_valid_stage(job)
                # hierarchy: BoltJob num_tries --> BoltRunner num_tries --> default
                max_tries = job.num_tries or self.num_tries
                while stage is not None:
                    tries = 0
                    while tries < max_tries:
                        tries += 1
                        try:
                            if await self.job_is_finished(job=job):
                                logger.info(
                                    # pyre-fixme: Undefined attribute [16]: `BoltCreateInstanceArgs` has no attribute `output_dir`
                                    f"Run for {job.job_name} completed. View results at {job.partner_bolt_args.create_instance_args.output_dir}"
                                )
                                return True
                            # disable retries if stage is not retryable by setting tries to max_tries+1
                            if not stage.is_retryable:
                                tries = max_tries + 1
                            await self.run_next_stage(
                                publisher_id=publisher_id,
                                partner_id=partner_id,
                                stage=stage,
                                poll_interval=job.poll_interval,
                                logger=logger,  # pyre-ignore
                            )
                            await self.wait_stage_complete(
                                publisher_id=publisher_id,
                                partner_id=partner_id,
                                stage=stage,
                                poll_interval=job.poll_interval,
                                logger=logger,  # pyre-ignore
                            )
                            break
                        except Exception as e:
                            if tries >= max_tries:
                                logger.exception(e)
                                return False
                            logger.error(f"Error: type: {type(e)}, message: {e}")
                            logger.info(
                                f"Retrying stage {stage}, Retries left: {self.num_tries - tries}."
                            )
                            await asyncio.sleep(RETRY_INTERVAL)
                    # update stage
                    stage = await self.get_next_valid_stage(job)
                results = await asyncio.gather(
                    *[
                        self.publisher_client.validate_results(
                            instance_id=publisher_id,
                            expected_result_path=job.publisher_bolt_args.expected_result_path,
                        ),
                        self.partner_client.validate_results(
                            instance_id=partner_id,
                            expected_result_path=job.partner_bolt_args.expected_result_path,
                        ),
                    ]
                )
                return all(results)
            except Exception as e:
                self.logger.exception(e)
                return False

    async def run_next_stage(
        self,
        publisher_id: str,
        partner_id: str,
        stage: PrivateComputationBaseStageFlow,
        poll_interval: int,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        logger = logger or self.logger
        publisher_status = (
            await self.publisher_client.update_instance(publisher_id)
        ).pc_instance_status
        if publisher_status not in [stage.started_status, stage.completed_status]:
            # don't retry if started or completed status
            logger.info(f"Publisher {publisher_id} starting stage {stage.name}.")
            await self.publisher_client.run_stage(instance_id=publisher_id, stage=stage)
        server_ips = None
        if stage.is_joint_stage:
            server_ips = await self.get_server_ips_after_start(
                instance_id=publisher_id,
                stage=stage,
                timeout=stage.timeout,
                poll_interval=poll_interval,
            )
            if server_ips is None:
                raise NoServerIpsException(
                    f"{stage.name} requires server ips but got none."
                )
        partner_status = (
            await self.partner_client.update_instance(partner_id)
        ).pc_instance_status
        if partner_status not in [stage.started_status, stage.completed_status]:
            # don't retry if started or completed status
            logger.info(f"Partner {partner_id} starting stage {stage.name}.")
            await self.partner_client.run_stage(
                instance_id=partner_id, stage=stage, server_ips=server_ips
            )

    async def get_server_ips_after_start(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        timeout: int,
        poll_interval: int,
    ) -> Optional[List[str]]:
        # Waits until stage has started status then updates stage and returns server ips
        start_time = time()
        while time() < start_time + timeout:
            state = await self.publisher_client.update_instance(instance_id)
            status = state.pc_instance_status
            if status is stage.started_status:
                return state.server_ips
            if status is stage.failed_status:
                raise StageFailedException(
                    f"{instance_id} waiting for status {stage.started_status}, got {status} instead.",
                )
            self.logger.info(
                f"{instance_id} current status is {status}, waiting for {stage.started_status}."
            )
            await asyncio.sleep(poll_interval)
        raise StageTimeoutException(
            f"Poll {instance_id} status timed out after {timeout}s expecting status {stage.started_status}."
        )

    async def wait_stage_complete(
        self,
        publisher_id: str,
        partner_id: str,
        stage: PrivateComputationBaseStageFlow,
        poll_interval: int,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        logger = logger or self.logger
        fail_status = stage.failed_status
        complete_status = stage.completed_status
        timeout = stage.timeout

        start_time = time()
        while time() < start_time + timeout:
            publisher_state, partner_state = await asyncio.gather(
                self.publisher_client.update_instance(instance_id=publisher_id),
                self.partner_client.update_instance(instance_id=partner_id),
            )
            if (
                publisher_state.pc_instance_status is complete_status
                and partner_state.pc_instance_status is complete_status
            ):
                # stages complete
                return
            if (
                publisher_state.pc_instance_status
                in [fail_status, PrivateComputationInstanceStatus.TIMEOUT]
                or partner_state.pc_instance_status is fail_status
            ):
                # stage failed, cancel partner side only in joint stage
                if stage.is_joint_stage:
                    try:
                        logger.error(
                            f"Publisher status: {publisher_state.pc_instance_status}. Canceling partner stage {stage.name}."
                        )
                        await self.partner_client.cancel_current_stage(
                            instance_id=partner_id
                        )
                    except Exception as e:
                        logger.error(
                            f"Unable to cancel current stage {stage.name}. Error: type: {type(e)}, message: {e}."
                        )
                raise StageFailedException(
                    f"Stage {stage.name} failed. Publisher status: {publisher_state.pc_instance_status}. Partner status: {partner_state.pc_instance_status}."
                )
            logger.info(
                f"Publisher {publisher_id} status is {publisher_state.pc_instance_status}, Partner {partner_id} status is {partner_state.pc_instance_status}. Waiting for status {complete_status}."
            )
            # keep polling
            await asyncio.sleep(poll_interval)
        raise StageTimeoutException(
            f"Stage {stage.name} timed out after {timeout}s. Publisher status: {publisher_state.pc_instance_status}. Partner status: {partner_state.pc_instance_status}."
        )

    async def _get_or_create_instances(self, job: BoltJob) -> Tuple[str, str]:
        """Checks to see if a job is new or being resumed

        If the job is new, it creates new instances and returns their IDs. If the job
        is being resumed, it returns the existing IDs.

        Args:
            - job: The job being run

        Returns:
            The existing publisher and partner IDs if the job is being resumed,
            or newly created publisher and partner IDs if the job is new.
        """
        if not self.skip_publisher_creation:
            resume_publisher_id = (
                job.publisher_bolt_args.create_instance_args.instance_id
            )
            resume_partner_id = job.partner_bolt_args.create_instance_args.instance_id
            if await self.publisher_client.is_existing_instance(
                instance_args=job.publisher_bolt_args.create_instance_args
            ) and await self.partner_client.is_existing_instance(
                instance_args=job.partner_bolt_args.create_instance_args
            ):
                # instance id already exists, we are resuming a run.
                publisher_id = resume_publisher_id
                partner_id = resume_partner_id
            else:
                # instance id does not exist, we should create new instances
                self.logger.info(f"[{job.job_name}] Creating instances...")
                publisher_id, partner_id = await asyncio.gather(
                    self.publisher_client.create_instance(
                        instance_args=job.publisher_bolt_args.create_instance_args
                    ),
                    self.partner_client.create_instance(
                        instance_args=job.partner_bolt_args.create_instance_args
                    ),
                )
        else:
            # GraphAPI client doesn't have access to instance_id before creation,
            # so for now we assume publisher is created/gotten by pl_study_runner
            # and an instance_id was passed into the job args
            publisher_id = job.publisher_bolt_args.create_instance_args.instance_id
            # check if partner should be created
            # note: publisher and partner should have the same id
            if await self.partner_client.is_existing_instance(
                instance_args=job.partner_bolt_args.create_instance_args
            ):
                partner_id = publisher_id
            else:
                partner_id = await self.partner_client.create_instance(
                    job.partner_bolt_args.create_instance_args
                )
        return publisher_id, partner_id

    async def job_is_finished(
        self,
        job: BoltJob,
    ) -> bool:
        publisher_id = job.publisher_bolt_args.create_instance_args.instance_id
        partner_id = job.partner_bolt_args.create_instance_args.instance_id
        publisher_status, partner_status = (
            state.pc_instance_status
            for state in await asyncio.gather(
                self.publisher_client.update_instance(publisher_id),
                self.partner_client.update_instance(partner_id),
            )
        )
        return job.is_finished(
            publisher_status=publisher_status, partner_status=partner_status
        )

    async def get_next_valid_stage(
        self, job: BoltJob
    ) -> Optional[PrivateComputationBaseStageFlow]:
        """Gets the next stage that should be run.

        Throws an IncompatibleStageError exception if stages are not
        compatible, e.g. partner is CREATED, publisher is PID_PREPARE_COMPLETED

        Args:
            - job: the job being run

        Returns:
            The next stage to be run, or None if the job is finished
        """

        if not await self.job_is_finished(job):
            publisher_id = job.publisher_bolt_args.create_instance_args.instance_id
            publisher_stage = await self.publisher_client.get_valid_stage(
                instance_id=publisher_id, stage_flow=job.stage_flow
            )
            partner_id = job.partner_bolt_args.create_instance_args.instance_id
            partner_stage = await self.partner_client.get_valid_stage(
                instance_id=partner_id, stage_flow=job.stage_flow
            )

            # this is expected for all joint stages
            if publisher_stage is partner_stage:
                return publisher_stage

            elif publisher_stage is None:
                return partner_stage
            elif partner_stage is None:
                return publisher_stage

            elif publisher_stage is partner_stage.previous_stage:
                publisher_status = (
                    await self.publisher_client.update_instance(publisher_id)
                ).pc_instance_status
                partner_status = (
                    await self.partner_client.update_instance(partner_id)
                ).pc_instance_status
                # if it's not a joint stage, the statuses don't matter at all since
                # each party operates independently
                # Example: publisher is RESHARD_FAILED, partner is RESHARD_COMPLETED
                if not publisher_stage.is_joint_stage or (
                    # it's fine if one party is completed and the other is started
                    # because the one with the started status just needs to call
                    # update_instance one more time
                    # Example: publisher is COMPUTATION_STARTED, partner is COMPUTATION_COMPLETED
                    job.stage_flow.is_started_status(publisher_status)
                    and job.stage_flow.is_completed_status(partner_status)
                ):
                    return publisher_stage
            elif partner_stage is publisher_stage.previous_stage:
                publisher_status = (
                    await self.publisher_client.update_instance(publisher_id)
                ).pc_instance_status
                partner_status = (
                    await self.partner_client.update_instance(partner_id)
                ).pc_instance_status
                # Example: publisher is RESHARD_COMPLETED, partner is RESHARD_FAILED
                if not partner_stage.is_joint_stage or (
                    # Example: publisher is COMPUTATION_COMPLETED, partner is COMPUTATION_STARTED
                    job.stage_flow.is_started_status(partner_status)
                    and job.stage_flow.is_completed_status(publisher_status)
                ):
                    return partner_stage
            # Example: partner is CREATED, publisher is PID_PREPARE_COMPLETED
            # Example: publisher is COMPUTATION COMPLETED, partner is PREPARE_COMPLETED
            # Example: publisher is COMPUTATION_COMPLETED, partner is COMPUTATION_FAILED
            raise IncompatibleStageError(
                f"Could not get next stage: Publisher status is {publisher_stage.name}, Partner status is {partner_stage.name}"
            )
        return None

    async def wait_valid_publisher_status(
        self, instance_id: str, poll_interval: int, timeout: int
    ) -> None:
        """Waits for publisher status to be valid

        Sometimes when resuming a run, the publisher status is TIMEOUT,
        UNKNOWN, or PROCESSING_REQUEST. We will try to run the stage
        to get a different status. This is a GraphAPI-only issue

        Args:
            - instance_id: Publisher instance_id
            - poll_interval: time in seconds between polls
            - timeout: timeout in seconds
        """

        status = (
            await self.publisher_client.update_instance(instance_id=instance_id)
        ).pc_instance_status
        if status in INVALID_STATUS_LIST:
            if status is PrivateComputationInstanceStatus.TIMEOUT:
                # no stage argument necessary for graphAPI
                await self.publisher_client.run_stage(instance_id=instance_id)
            start_time = time()
            while time() < start_time + timeout:
                status = (
                    await self.publisher_client.update_instance(instance_id)
                ).pc_instance_status
                if status not in INVALID_STATUS_LIST:
                    self.logger.info(f"Publisher instance has valid status: {status}.")
                    return
                self.logger.info(
                    f"Publisher instance status {status} invalid for calculation.\nPolling publisher instance expecting valid status."
                )
                await asyncio.sleep(poll_interval)
            raise WaitValidStatusTimeout(
                f"Timed out waiting for publisher {instance_id} valid status. Status: {status}"
            )
