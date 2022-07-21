#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from time import time
from typing import List, Optional, Tuple

from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs, BoltJob
from fbpcs.bolt.constants import (
    DEFAULT_MAX_PARALLEL_RUNS,
    DEFAULT_NUM_TRIES,
    RETRY_INTERVAL,
)
from fbpcs.bolt.exceptions import (
    NoServerIpsException,
    StageFailedException,
    StageTimeoutException,
)
from fbpcs.private_computation.entity.infra_config import PrivateComputationRole
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)

from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)


@dataclass
class BoltState:
    pc_instance_status: PrivateComputationInstanceStatus
    server_ips: Optional[List[str]] = None


class BoltClient(ABC):
    """
    Exposes async methods for creating instances, running stages, updating instances, and validating the correctness of a computation
    """

    @abstractmethod
    async def create_instance(self, instance_args: BoltCreateInstanceArgs) -> str:
        pass

    @abstractmethod
    async def run_stage(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        server_ips: Optional[List[str]] = None,
    ) -> None:
        pass

    @abstractmethod
    async def update_instance(self, instance_id: str) -> BoltState:
        pass

    @abstractmethod
    async def validate_results(
        self, instance_id: str, expected_result_path: Optional[str] = None
    ) -> bool:
        pass

    async def cancel_current_stage(self, instance_id: str) -> None:
        pass


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

    async def is_finished(
        self,
        publisher_id: str,
        partner_id: str,
        final_stage: PrivateComputationBaseStageFlow,
    ) -> bool:
        publisher_status = (
            await self.publisher_client.update_instance(publisher_id)
        ).pc_instance_status
        partner_status = (
            await self.partner_client.update_instance(partner_id)
        ).pc_instance_status
        return (publisher_status is final_stage.completed_status) and (
            partner_status is final_stage.completed_status
        )

    async def run_one(self, job: BoltJob) -> bool:
        async with self.semaphore:
            try:
                publisher_id, partner_id = await self._get_or_create_instances(job)

                # hierarchy: BoltJob num_tries --> BoltRunner num_tries --> default
                max_tries = job.num_tries or self.num_tries
                for stage in list(job.stage_flow)[1:]:
                    tries = 0
                    while tries < max_tries:
                        tries += 1
                        try:
                            final_stage = (
                                job.final_stage or job.stage_flow.get_last_stage()
                            )
                            if await self.is_finished(
                                publisher_id=publisher_id,
                                partner_id=partner_id,
                                final_stage=final_stage,
                            ):
                                self.logger.info(
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
                            )
                            await self.wait_stage_complete(
                                publisher_id=publisher_id,
                                partner_id=partner_id,
                                stage=stage,
                                poll_interval=job.poll_interval,
                            )
                            break
                        except Exception as e:
                            if tries >= max_tries:
                                self.logger.exception(e)
                                return False
                            self.logger.error(
                                f"Error: type: {type(e)}, message: {e}. Retries left: {self.num_tries - tries}."
                            )
                            await asyncio.sleep(RETRY_INTERVAL)
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
    ) -> None:
        publisher_status = (
            await self.publisher_client.update_instance(publisher_id)
        ).pc_instance_status
        server_ips = None
        if publisher_status not in [stage.started_status, stage.completed_status]:
            # don't retry if started or completed status
            self.logger.info(f"Publisher {publisher_id} starting stage {stage.name}.")
            await self.publisher_client.run_stage(instance_id=publisher_id, stage=stage)
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
            self.logger.info(f"Partner {partner_id} starting stage {stage.name}.")
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
    ) -> None:
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
                        self.logger.error(
                            f"Publisher status: {publisher_state.pc_instance_status}. Canceling partner stage {stage.name}."
                        )
                        await self.partner_client.cancel_current_stage(
                            instance_id=partner_id
                        )
                    except Exception as e:
                        self.logger.error(
                            f"Unable to cancel current stage {stage.name}. Error: type: {type(e)}, message: {e}."
                        )
                raise StageFailedException(
                    f"Stage {stage.name} failed. Publisher status: {publisher_state.pc_instance_status}. Partner status: {partner_state.pc_instance_status}."
                )
            self.logger.info(
                f"Publisher {publisher_id} status is {publisher_state.pc_instance_status}, Partner {partner_id} status is {partner_state.pc_instance_status}. Waiting for status {complete_status}."
            )
            # keep polling
            await asyncio.sleep(poll_interval)
        raise StageTimeoutException(
            f"Stage {stage.name} timed out after {timeout}s. Publisher status: {publisher_state.pc_instance_status}. Partner status: {partner_state.pc_instance_status}."
        )

    async def _is_existing_instance(
        self, instance_id: str, role: PrivateComputationRole
    ) -> bool:
        """Returns whether instance_id already exists for the given role

        Args:
            - instance_id: the id to be checked
            - role: publisher or partner

        Returns:
            True if there is an instance with instance_id for the given role, False otherwise
        """
        self.logger.info(f"Checking if {instance_id} exists...")
        try:
            if role is PrivateComputationRole.PUBLISHER:
                await self.publisher_client.update_instance(instance_id)
            else:
                await self.partner_client.update_instance(instance_id)
            self.logger.info(f"{instance_id} found.")
            return True
        except Exception:
            self.logger.info(f"{instance_id} not found.")
            return False

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
            if await self._is_existing_instance(
                instance_id=resume_publisher_id, role=PrivateComputationRole.PUBLISHER
            ) and await self._is_existing_instance(
                instance_id=resume_partner_id, role=PrivateComputationRole.PARTNER
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
            if await self._is_existing_instance(
                publisher_id, PrivateComputationRole.PARTNER
            ):
                partner_id = publisher_id
            else:
                partner_id = await self.partner_client.create_instance(
                    job.partner_bolt_args.create_instance_args
                )
        return publisher_id, partner_id
