#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import itertools
import logging
from time import time
from typing import Awaitable, Generic, List, Optional, Tuple, Type, TypeVar

from fbpcs.bolt.bolt_checkpoint import bolt_checkpoint

from fbpcs.bolt.bolt_client import BoltClient, BoltState

from fbpcs.bolt.bolt_hook import (
    BoltHookCommonInjectionArgs,
    BoltHookEvent,
    BoltHookKey,
    BoltHookTiming,
)
from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs, BoltJob
from fbpcs.bolt.bolt_job_summary import BoltJobSummary, BoltMetric, BoltMetricType
from fbpcs.bolt.bolt_summary import BoltSummary
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
from fbpcs.bolt.oss_bolt_pcs import BoltPCSCreateInstanceArgs
from fbpcs.infra.certificate.sample_tls_certificates import (
    SAMPLE_CA_CERTIFICATE,
    SAMPLE_SERVER_CERTIFICATE_BASE_DOMAIN,
)
from fbpcs.private_computation.entity.infra_config import PrivateComputationRole
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_status import (
    PrivateComputationInstanceStatus,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)

from fbpcs.utils.logger_adapter import LoggerAdapter

# Used to represent an "Awaitable"
A = TypeVar("A")

T = TypeVar("T", bound=BoltCreateInstanceArgs)
U = TypeVar("U", bound=BoltCreateInstanceArgs)
_IS_DYNAMIC_TLS_ENABLED = False


class BoltRunner(Generic[T, U]):
    def __init__(
        self,
        publisher_client: BoltClient[T],
        partner_client: BoltClient[U],
        max_parallel_runs: Optional[int] = None,
        num_tries: Optional[int] = None,
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

    async def run_async(
        self,
        jobs: List[BoltJob[T, U]],
    ) -> BoltSummary:
        start_time = time()
        results = list(await asyncio.gather(*[self.run_one(job=job) for job in jobs]))
        end_time = time()
        self.logger.info(f"BoltSummary: overall runtime: {end_time - start_time}")
        return BoltSummary(job_summaries=results)

    @bolt_checkpoint(
        dump_return_val=True,
    )
    async def run_one(self, job: BoltJob[T, U]) -> BoltJobSummary:
        bolt_metrics = []
        queue_start_time = time()
        async with self.semaphore:
            bolt_metrics.append(
                BoltMetric(
                    BoltMetricType.JOB_QUEUE_TIME,
                    time() - queue_start_time,
                )
            )
            job_start_time = time()
            try:
                publisher_id, partner_id = await asyncio.gather(
                    self.publisher_client.get_or_create_instance(
                        job.publisher_bolt_args.create_instance_args
                    ),
                    self.partner_client.get_or_create_instance(
                        job.partner_bolt_args.create_instance_args
                    ),
                )

                logger = LoggerAdapter(logger=self.logger, prefix=partner_id)
                await self.wait_valid_publisher_status(
                    instance_id=publisher_id,
                    poll_interval=job.poll_interval,
                    timeout=WAIT_VALID_STATUS_TIMEOUT,
                )
                stage_flow = await self.get_stage_flow(job=job)
                stage = await self.get_next_valid_stage(job=job, stage_flow=stage_flow)
                # hierarchy: BoltJob num_tries --> BoltRunner num_tries --> default
                max_tries = job.num_tries or self.num_tries
                while stage is not None:
                    stage_time = time()
                    # the following log is used by log_analyzer
                    logger.info(f"Valid stage found: {stage}")
                    tries = 0
                    while tries < max_tries:
                        tries += 1
                        try:
                            if await self.job_is_finished(
                                job=job, stage_flow=stage_flow
                            ):
                                logger.info(f"Run for {job.job_name} completed.")

                                if isinstance(
                                    job.partner_bolt_args.create_instance_args,
                                    BoltPCSCreateInstanceArgs,
                                ):
                                    logger.info(
                                        f"View {job.job_name} partner results at {job.partner_bolt_args.create_instance_args.output_dir}"
                                    )

                                if isinstance(
                                    job.publisher_bolt_args.create_instance_args,
                                    BoltPCSCreateInstanceArgs,
                                ):
                                    logger.info(
                                        f"View {job.job_name} publisher results at {job.publisher_bolt_args.create_instance_args.output_dir}"
                                    )
                                return BoltJobSummary(
                                    job_name=job.job_name,
                                    publisher_instance_id=job.publisher_bolt_args.create_instance_args.instance_id,
                                    partner_instance_id=job.partner_bolt_args.create_instance_args.instance_id,
                                    is_success=True,
                                    bolt_metrics=bolt_metrics,
                                )

                            # disable retries if stage is not retryable by setting tries to max_tries+1
                            if not stage.is_retryable:
                                tries = max_tries + 1

                            stage_startup_time = time()
                            next_stage_metrics = await self.run_next_stage(
                                publisher_id=publisher_id,
                                partner_id=partner_id,
                                stage=stage,
                                poll_interval=job.poll_interval,
                                logger=logger,
                            )
                            bolt_metrics.append(
                                BoltMetric(
                                    BoltMetricType.STAGE_START_UP_TIME,
                                    time() - stage_startup_time,
                                    stage,
                                )
                            )
                            bolt_metrics.extend(next_stage_metrics)

                            stage_wait_time = time()

                            await self._execute_event(
                                self.wait_stage_complete(
                                    publisher_id=publisher_id,
                                    partner_id=partner_id,
                                    stage=stage,
                                    poll_interval=job.poll_interval,
                                    logger=logger,
                                    stage_timeout_override=job.stage_timeout_override,
                                ),
                                job=job,
                                event=BoltHookEvent.STAGE_WAIT_FOR_COMPLETED,
                                stage=stage,
                                role=None,
                            )
                            bolt_metrics.append(
                                BoltMetric(
                                    BoltMetricType.STAGE_WAIT_FOR_COMPLETED,
                                    time() - stage_wait_time,
                                    stage,
                                )
                            )
                            break
                        except Exception as e:
                            if tries >= max_tries:
                                logger.exception(e)
                                bolt_metrics.append(
                                    BoltMetric(
                                        BoltMetricType.STAGE_TOTAL_RUNTIME,
                                        time() - stage_time,
                                        stage,
                                    )
                                )
                                return BoltJobSummary(
                                    job_name=job.job_name,
                                    publisher_instance_id=job.publisher_bolt_args.create_instance_args.instance_id,
                                    partner_instance_id=job.partner_bolt_args.create_instance_args.instance_id,
                                    is_success=False,
                                    bolt_metrics=bolt_metrics,
                                )
                            logger.error(f"Error: type: {type(e)}, message: {e}")
                            logger.info(
                                f"Retrying stage {stage}, Retries left: {self.num_tries - tries}."
                            )
                            await asyncio.sleep(RETRY_INTERVAL)
                    bolt_metrics.append(
                        BoltMetric(
                            BoltMetricType.STAGE_TOTAL_RUNTIME,
                            time() - stage_time,
                            stage,
                        )
                    )
                    # update stage
                    stage = await self.get_next_valid_stage(
                        job=job, stage_flow=stage_flow
                    )
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
                bolt_metrics.append(
                    BoltMetric(
                        BoltMetricType.JOB_RUN_TIME,
                        time() - job_start_time,
                    )
                )
                return BoltJobSummary(
                    job_name=job.job_name,
                    publisher_instance_id=job.publisher_bolt_args.create_instance_args.instance_id,
                    partner_instance_id=job.partner_bolt_args.create_instance_args.instance_id,
                    is_success=all(results),
                    bolt_metrics=bolt_metrics,
                )
            except Exception as e:
                self.logger.exception(e)
                return BoltJobSummary(
                    job_name=job.job_name,
                    publisher_instance_id=job.publisher_bolt_args.create_instance_args.instance_id,
                    partner_instance_id=job.partner_bolt_args.create_instance_args.instance_id,
                    is_success=False,
                    bolt_metrics=bolt_metrics,
                )

    async def _execute_event(
        self,
        awaitable: Awaitable[A],
        *,
        job: BoltJob[T, U],
        event: BoltHookEvent,
        stage: Optional[PrivateComputationBaseStageFlow] = None,
        role: Optional[PrivateComputationRole] = None,
    ) -> A:
        """Execute event, including an arbitrary awaitable and various hooks

        awaitable: Any awaitable to be executed (e.g. run_stage, wait_stage_complete)
        job: BoltJob containing publisher + partner arguments for the study/experiment
        event: A description of what the event is (e.g. wait for completed event)
        stage: Only run hooks for the given stage. If stage is None, run on all stages
        role: Only run hooks for the given role. If role is None, run for all roles

        Returns:
            The result from executing await awaitable
        """

        # run pre-event hook
        await self._run_hooks(
            job=job, event=event, when=BoltHookTiming.BEFORE, stage=stage, role=role
        )

        # run event hook + the awaitable
        res, _ = await asyncio.gather(
            awaitable,
            self._run_hooks(
                job=job, event=event, when=BoltHookTiming.DURING, stage=stage, role=role
            ),
        )

        # run post-event hook
        await self._run_hooks(
            job=job, event=event, when=BoltHookTiming.AFTER, stage=stage, role=role
        )

        return res

    @bolt_checkpoint(dump_params=True)
    async def _run_hooks(
        self,
        *,
        job: BoltJob[T, U],
        event: BoltHookEvent,
        when: BoltHookTiming,
        stage: Optional[PrivateComputationBaseStageFlow] = None,
        role: Optional[PrivateComputationRole] = None,
    ) -> None:
        all_hooks = (
            hook
            # get every combination of stage, role, when, and event.
            # "None" represents that the field isn't used to fetch the hooks
            for s, r, w, e in itertools.product(
                {stage, None}, {role, None}, {when, None}, {event, None}
            )
            for hook in job.hooks.get(
                BoltHookKey(event=e, when=w, stage=s.name if s else s, role=r), []
            )
        )

        await asyncio.gather(
            *[
                hook.inject(
                    BoltHookCommonInjectionArgs(
                        job=job,
                        publisher_client=self.publisher_client,
                        partner_client=self.partner_client,
                    )
                )
                for hook in all_hooks
            ]
        )

    @bolt_checkpoint(dump_params=True, include=["stage"])
    async def run_next_stage(
        self,
        publisher_id: str,
        partner_id: str,
        stage: PrivateComputationBaseStageFlow,
        poll_interval: int,
        logger: Optional[logging.Logger] = None,
    ) -> List[BoltMetric]:
        logger = logger or self.logger
        bolt_metrics = []
        publisher_time = time()
        if await self.publisher_client.should_invoke_stage(publisher_id, stage):
            logger.info(f"Publisher {publisher_id} starting stage {stage.name}.")
            await self.publisher_client.run_stage(instance_id=publisher_id, stage=stage)

        if await self.partner_client.should_invoke_stage(partner_id, stage):
            server_ips = None
            ca_certificate = None
            server_hostnames = None
            if stage.is_joint_stage:
                (
                    server_ips,
                    ca_certificate,
                    server_hostnames,
                ) = await self._get_publisher_state(
                    instance_id=publisher_id,
                    stage=stage,
                    timeout=stage.timeout,
                    poll_interval=poll_interval,
                )
                if server_ips is None:
                    raise NoServerIpsException(
                        f"{stage.name} requires server ips but got none."
                    )
            bolt_metrics.append(
                BoltMetric(
                    BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                    time() - publisher_time,
                    stage,
                    PrivateComputationRole.PUBLISHER,
                )
            )
            partner_time = time()
            logger.info(f"Partner {partner_id} starting stage {stage.name}.")
            await self.partner_client.run_stage(
                instance_id=partner_id,
                stage=stage,
                server_ips=server_ips,
                ca_certificate=ca_certificate,
                server_hostnames=server_hostnames,
            )
            bolt_metrics.append(
                BoltMetric(
                    BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                    time() - partner_time,
                    stage,
                    PrivateComputationRole.PARTNER,
                )
            )
        else:
            bolt_metrics.append(
                BoltMetric(
                    BoltMetricType.PLAYER_STAGE_START_UP_TIME,
                    time() - publisher_time,
                    stage,
                    PrivateComputationRole.PUBLISHER,
                )
            )
        return bolt_metrics

    @bolt_checkpoint(
        dump_params=True,
        include=["stage"],
        dump_return_val=True,
    )
    async def get_server_ips_after_start(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        timeout: int,
        poll_interval: int,
    ) -> Optional[List[str]]:
        """Gets state from the Publisher for coordination by the Partner side.

        NOTE: This method should be deprecated, the public interface removed in favor of _get_publisher_state().

        Throws an IncompatibleStageError exception if stages are not
        compatible, e.g. partner is CREATED, publisher is PID_PREPARE_COMPLETED

        Args:
            - instance_id: the study instance identifier
            - stage: the current stage
            - timeout: the request timeout, in seconds
            - poll_interval: the polling interval, in seconds

        Returns:
            A tuple representing the Publisher state, in the format: (server_ips, ca_certificate, server_hostnames)
        """
        (
            server_ips,
            ca_certificate,
            server_hostnames,
        ) = await self._get_publisher_state(
            instance_id=instance_id,
            stage=stage,
            timeout=stage.timeout,
            poll_interval=poll_interval,
        )
        return server_ips

    @bolt_checkpoint(
        dump_params=True,
        include=["stage"],
        dump_return_val=True,
    )
    async def _get_publisher_state(
        self,
        instance_id: str,
        stage: PrivateComputationBaseStageFlow,
        timeout: int,
        poll_interval: int,
    ) -> Tuple[Optional[List[str]], Optional[str], Optional[List[str]]]:
        """Gets state from the Publisher for coordination by the Partner side.

        Throws an IncompatibleStageError exception if stages are not
        compatible, e.g. partner is CREATED, publisher is PID_PREPARE_COMPLETED

        Args:
            - instance_id: the study instance identifier
            - stage: the current stage
            - timeout: the request timeout, in seconds
            - poll_interval: the polling interval, in seconds

        Returns:
            A tuple representing the Publisher state, in the format: (server_ips, ca_certificate, server_hostnames)
        """
        # only joint stage need to get server ips
        if not stage.is_joint_stage:
            return None, None, None

        # Waits until stage has started status then updates stage and returns server ips
        start_time = time()
        while time() < start_time + timeout:
            state = await self.publisher_client.update_instance(instance_id)
            status = state.pc_instance_status
            if status is stage.started_status:
                ca_certificate, server_hostnames = self._get_tls_config(state)

                return state.server_ips, ca_certificate, server_hostnames
            if status in [stage.failed_status, stage.completed_status]:
                # fast-fail on completed stage
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

    def _get_tls_config(
        self,
        state: BoltState,
    ) -> Tuple[Optional[str], Optional[List[str]]]:
        """Gets the TLS data from the Publisher state, if dynamic TLS enabled, otherwise data used for TLS testing.

        Args:
            - state: the Publisher state

        Returns:
            A tuple representing the Publisher's TLS configuration, in the format: (ca_certificate, server_hostnames)
        """
        if not _IS_DYNAMIC_TLS_ENABLED:
            # TODO: T136500624 remove option to disable dynamic TLS, and remove static/test TLS config values from codebase, once dynamic TLS is tested e2e
            # The certificate returned below does not provide any additional security and
            # is being used for intermediate testing purposes only.
            num_containers = len(state.server_ips) if state.server_ips else 1
            server_hostnames = [
                f"node{i}.{SAMPLE_SERVER_CERTIFICATE_BASE_DOMAIN}"
                for i in range(num_containers)
            ]
            return SAMPLE_CA_CERTIFICATE, server_hostnames

        return state.issuer_certificate, state.server_hostnames

    @bolt_checkpoint(dump_params=True, include=["stage"])
    async def wait_stage_complete(
        self,
        publisher_id: str,
        partner_id: str,
        stage: PrivateComputationBaseStageFlow,
        poll_interval: int,
        logger: Optional[logging.Logger] = None,
        stage_timeout_override: Optional[int] = None,
    ) -> None:
        logger = logger or self.logger
        fail_status = stage.failed_status
        complete_status = stage.completed_status
        timeout = (
            stage.timeout
            if not stage_timeout_override
            else max(stage.timeout, stage_timeout_override)
        )

        start_time = time()
        publisher_state, partner_state = None, None
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
                try:
                    await asyncio.gather(
                        self.publisher_client.log_failed_containers(publisher_id),
                        self.partner_client.log_failed_containers(partner_id),
                    )
                except Exception as e:
                    logger.debug(e)
                # stage failed, cancel publisher and partner side only in joint stage
                if stage.is_joint_stage:
                    try:
                        is_coretry_enabled = await self.partner_client.has_feature(
                            partner_id, PCSFeature.PC_COORDINATED_RETRY
                        )
                        if is_coretry_enabled:
                            logger.error(
                                f"Publisher status: {publisher_state.pc_instance_status}. Canceling publisher and partner stage {stage.name}."
                            )
                            await asyncio.gather(
                                self.publisher_client.cancel_current_stage(
                                    instance_id=publisher_id
                                ),
                                self.partner_client.cancel_current_stage(
                                    instance_id=partner_id
                                ),
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
            f"Stage {stage.name} timed out after {timeout}s. Publisher status: {publisher_state.pc_instance_status if publisher_state else 'Not Found'}. Partner status: {partner_state.pc_instance_status if partner_state else 'Not Found'}."
        )

    @bolt_checkpoint(
        dump_return_val=True,
    )
    async def job_is_finished(
        self,
        job: BoltJob[T, U],
        stage_flow: Type[PrivateComputationBaseStageFlow],
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
            publisher_status=publisher_status,
            partner_status=partner_status,
            stage_flow=stage_flow,
        )

    @bolt_checkpoint(
        dump_return_val=True,
    )
    async def get_stage_flow(
        self,
        job: BoltJob[T, U],
    ) -> Type[PrivateComputationBaseStageFlow]:
        publisher_id = job.publisher_bolt_args.create_instance_args.instance_id
        partner_id = job.partner_bolt_args.create_instance_args.instance_id

        publisher_stage_flow, partner_stage_flow = await asyncio.gather(
            self.publisher_client.get_stage_flow(instance_id=publisher_id),
            self.partner_client.get_stage_flow(instance_id=partner_id),
        )
        if (
            publisher_stage_flow
            and partner_stage_flow
            and publisher_stage_flow != partner_stage_flow
        ):
            raise IncompatibleStageError(
                f"Publisher and Partner should be running in same Stage flow: Publisher is {publisher_stage_flow.get_cls_name()}, Partner is {partner_stage_flow.get_cls_name()}"
            )
        elif publisher_stage_flow is None and partner_stage_flow is None:
            # both stage flow are not exist
            raise IncompatibleStageError(
                f"Could not get stage flow: Publisher id is {publisher_id}, Partner id is {partner_id}"
            )

        # pyre-ignore Incompatible return type [7]
        return partner_stage_flow or publisher_stage_flow

    @bolt_checkpoint(
        dump_return_val=True,
    )
    async def get_next_valid_stage(
        self,
        job: BoltJob[T, U],
        stage_flow: Type[PrivateComputationBaseStageFlow],
    ) -> Optional[PrivateComputationBaseStageFlow]:
        """Gets the next stage that should be run.

        Throws an IncompatibleStageError exception if stages are not
        compatible, e.g. partner is CREATED, publisher is PID_PREPARE_COMPLETED

        Args:
            - job: the job being run

        Returns:
            The next stage to be run, or None if the job is finished
        """
        if not await self.job_is_finished(job=job, stage_flow=stage_flow):
            publisher_id = job.publisher_bolt_args.create_instance_args.instance_id
            publisher_stage = await self.publisher_client.get_valid_stage(
                instance_id=publisher_id, stage_flow=stage_flow
            )
            partner_id = job.partner_bolt_args.create_instance_args.instance_id
            partner_stage = await self.partner_client.get_valid_stage(
                instance_id=partner_id, stage_flow=stage_flow
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
                if stage_flow.is_completed_status(partner_status) and (
                    not publisher_stage.is_joint_stage
                    # it's fine if one party is completed and the other is started
                    # because the one with the started status just needs to call
                    # update_instance one more time
                    # Example: publisher is COMPUTATION_STARTED, partner is COMPUTATION_COMPLETED
                    or stage_flow.is_started_status(publisher_status)
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
                if stage_flow.is_completed_status(publisher_status) and (
                    not partner_stage.is_joint_stage
                    # Example: publisher is COMPUTATION_COMPLETED, partner is COMPUTATION_STARTED
                    or stage_flow.is_started_status(partner_status)
                ):
                    return partner_stage
            # Example: partner is CREATED, publisher is PID_PREPARE_COMPLETED
            # Example: publisher is COMPUTATION COMPLETED, partner is PREPARE_COMPLETED
            # Example: publisher is COMPUTATION_COMPLETED, partner is COMPUTATION_FAILED
            raise IncompatibleStageError(
                f"Could not get next stage: Publisher status is {publisher_stage.name}, Partner status is {partner_stage.name}"
            )
        return None

    @bolt_checkpoint()
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
