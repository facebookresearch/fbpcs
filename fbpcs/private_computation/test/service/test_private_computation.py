#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
import os
import random
import time
import unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from unittest import mock
from unittest.mock import AsyncMock, call, MagicMock, Mock, patch

from fbpcp.entity.container_instance import ContainerInstance, ContainerInstanceStatus

from fbpcp.error.pcp import ThrottlingError
from fbpcp.service.onedocker import OneDockerService
from fbpcs.common.entity.pcs_mpc_instance import PCSMPCInstance
from fbpcs.common.entity.stage_state_instance import (
    StageStateInstance,
    StageStateInstanceStatus,
)
from fbpcs.infra.certificate.null_certificate_provider import NullCertificateProvider
from fbpcs.infra.certificate.sample_tls_certificates import (
    SAMPLE_CA_CERTIFICATE,
    SAMPLE_SERVER_CERTIFICATE,
)
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_binary_names import OneDockerBinaryNames
from fbpcs.onedocker_service_config import OneDockerServiceConfig
from fbpcs.private_computation.entity.infra_config import (
    InfraConfig,
    PrivateComputationGameType,
    StatusUpdate,
    UnionedPCInstance,
)
from fbpcs.private_computation.entity.pc_validator_config import PCValidatorConfig
from fbpcs.private_computation.entity.pcs_feature import PCSFeature
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
    PrivateComputationInstanceStatus,
    PrivateComputationRole,
)
from fbpcs.private_computation.entity.product_config import (
    AggregationType,
    AttributionRule,
    CommonProductConfig,
    LiftConfig,
    ProductConfig,
)
from fbpcs.private_computation.repository.private_computation_game import GameNames
from fbpcs.private_computation.service.constants import (
    DEFAULT_K_ANONYMITY_THRESHOLD_PA,
    DEFAULT_K_ANONYMITY_THRESHOLD_PL,
    DEFAULT_LOG_COST_TO_S3,
    FBPCS_BUNDLE_ID,
    NUM_NEW_SHARDS_PER_FILE,
)
from fbpcs.private_computation.service.errors import (
    PrivateComputationServiceInvalidStageError,
    PrivateComputationServiceValidationError,
)
from fbpcs.private_computation.service.mpc.mpc import (
    MPCInstanceStatus,
    MPCParty,
    MPCService,
)
from fbpcs.private_computation.service.pcf2_attribution_stage_service import (
    PCF2AttributionStageService,
)
from fbpcs.private_computation.service.pid_prepare_stage_service import (
    PIDPrepareStageService,
)
from fbpcs.private_computation.service.pid_run_protocol_stage_service import (
    PIDRunProtocolStageService,
)
from fbpcs.private_computation.service.pid_shard_stage_service import (
    PIDShardStageService,
)

from fbpcs.private_computation.service.private_computation import (
    PCSERVICE_ENTITY_NAME,
    PrivateComputationService,
)
from fbpcs.private_computation.service.private_computation_stage_service import (
    PrivateComputationStageService,
)
from fbpcs.private_computation.service.utils import (
    create_and_start_mpc_instance,
    DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
    map_private_computation_role_to_mpc_party,
    transform_file_path,
)
from fbpcs.private_computation.stage_flows.private_computation_base_stage_flow import (
    PrivateComputationBaseStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_mr_pid_pcf2_lift_stage_flow import (
    PrivateComputationMrPidPCF2LiftStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_mr_stage_flow import (
    PrivateComputationMRStageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_pcf2_stage_flow import (
    PrivateComputationPCF2StageFlow,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)
from fbpcs.private_computation.stage_flows.stage_selector import StageSelector
from fbpcs.private_computation.test.service.dummy_stage_flow import DummyStageFlow


def _get_valid_stages_data() -> List[Tuple[PrivateComputationBaseStageFlow]]:
    return [
        (PrivateComputationStageFlow.ID_MATCH,),
        (PrivateComputationStageFlow.COMPUTE,),
        (PrivateComputationStageFlow.AGGREGATE,),
        (PrivateComputationStageFlow.POST_PROCESSING_HANDLERS,),
        (PrivateComputationPCF2StageFlow.ID_MATCH,),
        (PrivateComputationPCF2StageFlow.PCF2_ATTRIBUTION,),
        (PrivateComputationPCF2StageFlow.PCF2_AGGREGATION,),
        (PrivateComputationPCF2StageFlow.AGGREGATE,),
    ]


class TestPrivateComputationService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        container_svc_patcher = patch("fbpcp.service.container_aws.AWSContainerService")
        storage_svc_patcher = patch("fbpcp.service.storage_s3.S3StorageService")
        mpc_instance_repo_patcher = patch(
            "fbpcs.common.repository.mpc_instance_local.LocalMPCInstanceRepository"
        )
        private_computation_instance_repo_patcher = patch(
            "fbpcs.private_computation.repository.private_computation_instance_local.LocalPrivateComputationInstanceRepository"
        )
        mpc_game_svc_patcher = patch(
            "fbpcs.private_computation.service.mpc.mpc_game.MPCGameService"
        )
        container_svc = container_svc_patcher.start()
        storage_svc = storage_svc_patcher.start()
        mpc_instance_repository = mpc_instance_repo_patcher.start()
        private_computation_instance_repository = (
            private_computation_instance_repo_patcher.start()
        )
        mpc_game_svc = mpc_game_svc_patcher.start()

        for patcher in (
            container_svc_patcher,
            storage_svc_patcher,
            mpc_instance_repo_patcher,
            private_computation_instance_repo_patcher,
            mpc_game_svc_patcher,
        ):
            self.addCleanup(patcher.stop)

        self.onedocker_service_config = OneDockerServiceConfig(
            task_definition="test_task_definition",
        )

        self.onedocker_binary_config_map = defaultdict(
            lambda: OneDockerBinaryConfig(
                tmp_directory="/test_tmp_directory/",
                binary_version="latest",
                repository_path="test_path/",
            )
        )

        self.onedocker_service = OneDockerService(
            container_svc, self.onedocker_service_config.task_definition
        )

        self.mpc_service = MPCService(
            container_svc=container_svc,
            instance_repository=mpc_instance_repository,
            task_definition="test_task_definition",
            mpc_game_svc=mpc_game_svc,
        )

        self.pc_validator_config = PCValidatorConfig(
            region="us-west-2",
        )

        self.private_computation_service = PrivateComputationService(
            instance_repository=private_computation_instance_repository,
            storage_svc=storage_svc,
            mpc_svc=self.mpc_service,
            onedocker_svc=self.onedocker_service,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
            pc_validator_config=self.pc_validator_config,
        )

        self.test_private_computation_id = "test_private_computation_id"
        self.test_num_containers = 2
        self.test_input_path = "in_path"
        self.test_output_dir = "out_dir"
        self.test_game_type = PrivateComputationGameType.LIFT
        self.test_concurrency = 1
        self.log_cost_bucket = "test_log_bucket"
        self.test_hmac_key = "CoXbp7BOEvAN9L1CB2DAORHHr3hB7wE7tpxMYm07tc0="

    @mock.patch("time.time", new=mock.MagicMock(return_value=1))
    def test_create_instance(self) -> None:
        for test_game_type, expected_k_anon, pcs_features, test_role in (
            (
                PrivateComputationGameType.LIFT,
                DEFAULT_K_ANONYMITY_THRESHOLD_PL,
                [PCSFeature.PCS_DUMMY.value, PCSFeature.UNKNOWN.value],
                PrivateComputationRole.PUBLISHER,
            ),
            (
                PrivateComputationGameType.ATTRIBUTION,
                DEFAULT_K_ANONYMITY_THRESHOLD_PA,
                None,
                PrivateComputationRole.PUBLISHER,
            ),
            # test PCSFeature.PRIVATE_ATTRIBUTION_MR_PID for attribution with publisher
            (
                PrivateComputationGameType.ATTRIBUTION,
                DEFAULT_K_ANONYMITY_THRESHOLD_PA,
                [
                    PCSFeature.PCS_DUMMY.value,
                    PCSFeature.PRIVATE_ATTRIBUTION_MR_PID.value,
                ],
                PrivateComputationRole.PUBLISHER,
            ),
            # test PCSFeature.PRIVATE_ATTRIBUTION_MR_PID for lift with publisher
            (
                PrivateComputationGameType.LIFT,
                DEFAULT_K_ANONYMITY_THRESHOLD_PL,
                [
                    PCSFeature.PCS_DUMMY.value,
                    PCSFeature.PRIVATE_ATTRIBUTION_MR_PID.value,
                ],
                PrivateComputationRole.PUBLISHER,
            ),
            # test PCSFeature.PCF_TLS for lift with publisher
            (
                PrivateComputationGameType.LIFT,
                DEFAULT_K_ANONYMITY_THRESHOLD_PL,
                [
                    PCSFeature.PCS_DUMMY.value,
                    PCSFeature.PCF_TLS.value,
                ],
                PrivateComputationRole.PUBLISHER,
            ),
            # test PCSFeature.PCF_TLS for lift with partner
            (
                PrivateComputationGameType.LIFT,
                DEFAULT_K_ANONYMITY_THRESHOLD_PL,
                [
                    PCSFeature.PCS_DUMMY.value,
                    PCSFeature.PCF_TLS.value,
                ],
                PrivateComputationRole.PARTNER,
            ),
        ):
            with self.subTest(
                test_game_type=test_game_type,
                expected_k_anon=expected_k_anon,
                pcs_features=pcs_features,
            ):
                self.private_computation_service.create_instance(
                    instance_id=self.test_private_computation_id,
                    role=test_role,
                    game_type=test_game_type,
                    input_path=self.test_input_path,
                    output_dir=self.test_output_dir,
                    num_pid_containers=self.test_num_containers,
                    num_mpc_containers=self.test_num_containers,
                    concurrency=self.test_concurrency,
                    num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
                    hmac_key=self.test_hmac_key,
                    attribution_rule=AttributionRule.LAST_CLICK_1D,
                    aggregation_type=AggregationType.MEASUREMENT,
                    pcs_features=pcs_features,
                )
                # check instance_repository.create is called with the correct arguments
                # pyre-fixme[16]: Callable `create` has no attribute `assert_called`.
                self.private_computation_service.instance_repository.create.assert_called()
                # pyre-fixme[16]: Callable `create` has no attribute `call_args`.
                args = self.private_computation_service.instance_repository.create.call_args.kwargs[
                    "instance"
                ]
                self.assertEqual(
                    self.test_private_computation_id, args.infra_config.instance_id
                )
                self.assertEqual(test_role, args.infra_config.role)
                self.assertEqual(
                    PrivateComputationInstanceStatus.CREATED, args.infra_config.status
                )
                self.assertEqual(1, args.infra_config.creation_ts)
                if test_game_type is PrivateComputationGameType.LIFT:
                    self.assertEqual(
                        expected_k_anon, args.product_config.k_anonymity_threshold
                    )

                yesterday_date = datetime.now(tz=timezone.utc) - timedelta(days=1)
                yesterday_timestamp = datetime.timestamp(yesterday_date)
                self.assertAlmostEqual(
                    int(yesterday_timestamp),
                    args.product_config.common.post_processing_data.dataset_timestamp,
                )
                self.assertEqual(args.infra_config.server_key_ref, None)
                if pcs_features is not None:
                    if PCSFeature.PRIVATE_ATTRIBUTION_MR_PID.value in pcs_features:
                        self.assertTrue(
                            args.has_feature(PCSFeature.PRIVATE_ATTRIBUTION_MR_PID)
                        )
                    elif PCSFeature.PCF_TLS.value not in pcs_features:
                        self.assertTrue(args.has_feature(PCSFeature.PCS_DUMMY))
                        # feature flags is unsorted
                        self.assertEqual(
                            set("pcs_dummy_feature,unknown".split(",")),
                            set(args.feature_flags.split(",")),
                        )
                else:
                    self.assertFalse(args.has_feature(PCSFeature.PCS_DUMMY))
                    self.assertEqual(None, args.feature_flags)

                # assert PCSFeature.PRIVATE_ATTRIBUTION_MR_PID will override stage flow cls
                if (
                    pcs_features is not None
                    and PCSFeature.PRIVATE_ATTRIBUTION_MR_PID.value in pcs_features
                ):
                    self.assertEqual(
                        args.stage_flow,
                        PrivateComputationMRStageFlow
                        if test_game_type is PrivateComputationGameType.ATTRIBUTION
                        else PrivateComputationMrPidPCF2LiftStageFlow,
                    )
                elif test_game_type is PrivateComputationGameType.ATTRIBUTION:
                    self.assertEqual(
                        args.stage_flow,
                        PrivateComputationPCF2StageFlow,
                    )
                else:
                    self.assertEqual(args.stage_flow, PrivateComputationStageFlow)

                # assert PCSFeature.PCF_TLS will generate certificates and store
                # them in InfraConfig
                if (
                    pcs_features is not None
                    and PCSFeature.PCF_TLS.value in pcs_features
                    and test_role is PrivateComputationRole.PUBLISHER
                ):
                    self.assertEqual(
                        args.infra_config.server_certificate,
                        SAMPLE_SERVER_CERTIFICATE,
                    )
                    self.assertEqual(
                        args.infra_config.ca_certificate,
                        SAMPLE_CA_CERTIFICATE,
                    )
                    self.assertEqual(
                        args.infra_config.server_domain,
                        "study123.pci.facebook.com",
                    )

                if (
                    pcs_features is not None
                    and PCSFeature.PCF_TLS.value in pcs_features
                    and test_role is PrivateComputationRole.PARTNER
                ):
                    self.assertEqual(
                        args.infra_config.server_certificate,
                        None,
                    )
                    self.assertEqual(
                        args.infra_config.ca_certificate,
                        SAMPLE_CA_CERTIFICATE,
                    )
                    self.assertEqual(
                        args.infra_config.server_domain,
                        None,
                    )

    @mock.patch("time.time", new=mock.MagicMock(return_value=1))
    def test_create_instance_mr_workflow(self) -> None:
        test_role = PrivateComputationRole.PUBLISHER
        for test_game_type, expected_k_anon in (
            (PrivateComputationGameType.LIFT, DEFAULT_K_ANONYMITY_THRESHOLD_PL),
            (PrivateComputationGameType.ATTRIBUTION, DEFAULT_K_ANONYMITY_THRESHOLD_PA),
        ):
            with self.subTest(
                test_game_type=test_game_type, expected_k_anon=expected_k_anon
            ):
                instance = self.private_computation_service.create_instance(
                    instance_id=self.test_private_computation_id,
                    role=test_role,
                    game_type=test_game_type,
                    input_path=self.test_input_path,
                    output_dir=self.test_output_dir,
                    num_pid_containers=self.test_num_containers,
                    num_mpc_containers=self.test_num_containers,
                    concurrency=self.test_concurrency,
                    num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
                    hmac_key=self.test_hmac_key,
                    stage_flow_cls=PrivateComputationMRStageFlow,
                    attribution_rule=AttributionRule.LAST_CLICK_1D,
                    aggregation_type=AggregationType.MEASUREMENT,
                )
                # check instance_repository.create is called with the correct arguments
                # pyre-fixme[16]: Callable `create` has no attribute `assert_called`.
                self.private_computation_service.instance_repository.create.assert_called()
                # pyre-fixme[16]: Callable `create` has no attribute `call_args`.
                args = self.private_computation_service.instance_repository.create.call_args.kwargs[
                    "instance"
                ]
                self.assertEqual(
                    instance.get_flow_cls_name,
                    "PrivateComputationMRStageFlow",
                )

                self.assertEqual(
                    self.test_private_computation_id, args.infra_config.instance_id
                )
                self.assertEqual(test_role, args.infra_config.role)
                self.assertEqual(
                    PrivateComputationInstanceStatus.CREATED, args.infra_config.status
                )
                self.assertEqual(1, args.infra_config.creation_ts)
                if test_game_type is PrivateComputationGameType.LIFT:
                    self.assertEqual(
                        expected_k_anon, args.product_config.k_anonymity_threshold
                    )

    @mock.patch("time.time", new=mock.MagicMock(side_effect=range(1, 100)))
    def test_update_instance(self) -> None:
        mock_metric_svc = MagicMock()
        self.private_computation_service.metric_svc = mock_metric_svc
        stage_state_instance = StageStateInstance(
            instance_id=self.test_private_computation_id,
            stage_name="test_stage",
            containers=[],
        )
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.ID_MATCHING_STARTED,
            instances=[stage_state_instance],
        )
        pid_run_protocol_stage_svc = PIDRunProtocolStageService(
            storage_svc=self.private_computation_service.storage_svc,
            onedocker_svc=self.onedocker_service,
            onedocker_binary_config_map=self.onedocker_binary_config_map,
        )
        StageSelector.get_stage_service = MagicMock(
            return_value=pid_run_protocol_stage_svc
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )
        new_status = PrivateComputationInstanceStatus.ID_MATCHING_COMPLETED
        pid_run_protocol_stage_svc.get_status = MagicMock(return_value=new_status)

        # end_ts should not be calculated until the instance run is complete.
        self.assertEqual(0, private_computation_instance.infra_config.end_ts)

        # call update on the PrivateComputationInstance
        updated_instance = self.private_computation_service.update_instance(
            instance_id=self.test_private_computation_id
        )
        # check update instance called on the right private lift instance
        # pyre-fixme[16]: Callable `update` has no attribute `assert_called_with`.
        self.private_computation_service.instance_repository.update.assert_called_with(
            instance=private_computation_instance
        )
        # check updated_instance has new status
        self.assertEqual(
            new_status,
            updated_instance.infra_config.status,
        )

        # create one MPC instance to be put into PrivateComputationInstance
        test_mpc_id = "test_mpc_id"
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=GameNames.LIFT.value,
            mpc_party=MPCParty.SERVER,
            num_workers=2,
        )

        initialized_time = int(time.time())
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_INITIALIZED,
            instances=[mpc_instance],
            status_updates=[
                StatusUpdate(
                    status=PrivateComputationInstanceStatus.COMPUTATION_INITIALIZED,
                    status_update_ts=initialized_time,
                )
            ],
        )

        updated_mpc_instance = mpc_instance
        updated_mpc_instance.status = MPCInstanceStatus.COMPLETED
        self.private_computation_service.mpc_svc.update_instance = MagicMock(
            return_value=updated_mpc_instance
        )

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )
        # call update on the PrivateComputationInstance
        updated_instance = self.private_computation_service.update_instance(
            instance_id=self.test_private_computation_id
        )

        # check update instance called on the right mpc instance
        # pyre-fixme[16]: Callable `update_instance` has no attribute `assert_called`.
        self.private_computation_service.mpc_svc.update_instance.assert_called()
        self.assertEqual(
            test_mpc_id,
            # pyre-fixme[16]: Callable `update_instance` has no attribute `call_args`.
            self.private_computation_service.mpc_svc.update_instance.call_args[0][0],
        )

        # check update instance called on the right private lift instance
        self.private_computation_service.instance_repository.update.assert_called_with(
            instance=private_computation_instance
        )

        # check updated_instance has new status
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
            updated_instance.infra_config.status,
        )

        # elapsed_time should report current running time if the run is incomplete.
        self.assertEqual(
            time.time() - private_computation_instance.infra_config.creation_ts + 1,
            private_computation_instance.elapsed_time,
        )
        mock_metric_svc.bump_entity_key_avg.assert_called_with(
            PCSERVICE_ENTITY_NAME,
            f"{private_computation_instance.current_stage.name}.time_ms",
            (
                private_computation_instance.infra_config.status_update_ts
                - initialized_time
            )
            * 1000,
        )

        before_end_time = time.time()
        before_update_time = private_computation_instance.infra_config.status_update_ts
        private_computation_instance.update_status(
            private_computation_instance.stage_flow.get_last_stage().completed_status,
            logging.getLogger(),
        )
        after_end_time = time.time()
        after_update_time = private_computation_instance.infra_config.status_update_ts
        # We have this somewhat complicated assert because `time.time` will be called
        # multiple times inside the `update_status` statement and don't want any
        # *really* specific testing like "end_ts = time.time() + 2" since that would
        # be testing a side-effect rather than actual usage. Instead, we just check
        # that the end time happened in between the time before and after the call.
        self.assertTrue(
            before_end_time
            < private_computation_instance.infra_config.end_ts
            < after_end_time
        )
        expected_elapsed_time = (
            private_computation_instance.infra_config.end_ts
            - private_computation_instance.infra_config.creation_ts
        )
        self.assertEqual(
            expected_elapsed_time,
            private_computation_instance.elapsed_time,
        )
        self.assertEqual(
            private_computation_instance.get_status_elapsed_time(
                start_status=PrivateComputationInstanceStatus.COMPUTATION_COMPLETED,
                end_status=private_computation_instance.stage_flow.get_last_stage().completed_status,
            ),
            after_update_time - before_update_time,
        )
        self.assertEqual(updated_mpc_instance.server_uris, None)

    def test_update_instance_throttling_error(self) -> None:
        # Arrange
        instance_mock = MagicMock(
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            current_stage=MagicMock(
                get_stage_service=MagicMock(
                    return_value=MagicMock(
                        get_status=MagicMock(side_effect=ThrottlingError())
                    )
                )
            ),
        )
        self.private_computation_service.logger = MagicMock()
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=instance_mock
        )

        # Act
        self.private_computation_service.update_instance(instance_id="1")

        # Assert
        # Asserting for the specific log message is a bit too prescriptive,
        # but knowing that we logged a warning and *didn't* raise an error is
        # the most important thing.
        instance_mock.current_stage.get_stage_service().get_status.assert_called_once()
        self.private_computation_service.logger.warning.assert_called_once()

    @staticmethod
    def _get_dummy_stage_svc() -> PrivateComputationStageService:
        """create a DummyTestStageService class and instantiate an instance of it"""

        return type(
            "DummyTestStageService",
            (PrivateComputationStageService,),
            {
                "run_async": AsyncMock(
                    # run_async will return whatever pc_instance privatelift.run_stage passes it
                    side_effect=lambda pc_instance, *args, **kwargs: pc_instance
                ),
                "get_status": Mock(
                    # run_async will return whatever pc_instance privatelift.run_stage passes it
                    side_effect=lambda pc_instance, *args, **kwargs: pc_instance.status
                ),
                "stop_service": Mock(
                    # run_async will return whatever pc_instance privatelift.run_stage passes it
                    side_effect=lambda pc_instance, *args, **kwargs: None
                ),
            },
        )()

    def test_get_next_runnable_stage_completed_status(self) -> None:
        flow = PrivateComputationStageFlow
        # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
        status = flow.ID_MATCH.previous_stage.completed_status

        instance = self.create_sample_instance(status)

        self.assertEqual(flow.ID_MATCH, instance.get_next_runnable_stage())

    def test_get_next_runnable_stage_failed_status(self) -> None:
        flow = PrivateComputationStageFlow
        status = flow.ID_MATCH.failed_status

        instance = self.create_sample_instance(status)

        self.assertEqual(flow.ID_MATCH, instance.get_next_runnable_stage())

    def test_get_next_runnable_stage_started_status(self) -> None:
        flow = PrivateComputationStageFlow
        status = flow.ID_MATCH.started_status

        instance = self.create_sample_instance(status)

        self.assertEqual(None, instance.get_next_runnable_stage())

    def test_get_next_runnable_stage_nothing_left(self) -> None:
        flow = PrivateComputationStageFlow
        status = flow.get_last_stage().completed_status

        instance = self.create_sample_instance(status)

        self.assertEqual(None, instance.get_next_runnable_stage())

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService.run_stage_async"
    )
    def test_run_next(self, mock_run_stage_async) -> None:
        flow = PrivateComputationStageFlow
        # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
        status = flow.ID_MATCH.previous_stage.completed_status

        instance = self.create_sample_instance(status)

        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=instance
        )
        self.private_computation_service.run_next(instance.infra_config.instance_id)
        mock_run_stage_async.assert_called_with(
            instance.infra_config.instance_id, flow.ID_MATCH, server_ips=None
        )

    @mock.patch(
        "fbpcs.private_computation.service.private_computation.PrivateComputationService.run_stage_async"
    )
    def test_run_next_ignore_stage_flow_completed(self, mock_run_stage_async) -> None:
        flow = PrivateComputationStageFlow
        status = flow.get_last_stage().completed_status

        instance = self.create_sample_instance(status)

        with self.assertRaises(PrivateComputationServiceInvalidStageError):
            self.private_computation_service.run_next(instance.infra_config.instance_id)

        mock_run_stage_async.assert_not_called()

    def test_run_stage_correct_stage_order(
        self,
    ) -> None:
        """
        tests that run_stage runs stage_svc when the stage_svc is the next stage in the sequence
        """

        def _run_sub_test(stage: PrivateComputationBaseStageFlow):
            ################# PREVIOUS STAGE COMPLETED OR RETRY #######################
            stage_svc = self._get_dummy_stage_svc()
            for status in (
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                stage.previous_stage.completed_status,
                stage.failed_status,
            ):
                pl_instance = self.create_sample_instance(status=status)
                self.private_computation_service.instance_repository.read = MagicMock(
                    return_value=pl_instance
                )

                pl_instance = self.private_computation_service.run_stage(
                    pl_instance.infra_config.instance_id, stage, stage_svc
                )
                self.assertEqual(
                    pl_instance.infra_config.status, stage.initialized_status
                )

        for data_test in _get_valid_stages_data():
            stage = data_test[0]
            with self.subTest(stage=stage):
                _run_sub_test(stage)

    def test_run_stage_status_already_started(
        self,
    ) -> None:
        """
        tests that run_stage does not run stage_svc when the instance status is already started
        """

        def _run_sub_test(stage: PrivateComputationBaseStageFlow):
            ################# CURRENT STAGE STATUS NOT VALID #######################
            stage_svc = self._get_dummy_stage_svc()
            pl_instance = self.create_sample_instance(status=stage.started_status)

            self.private_computation_service.instance_repository.read = MagicMock(
                return_value=pl_instance
            )

            with self.assertRaises(ValueError):
                pl_instance = self.private_computation_service.run_stage(
                    pl_instance.infra_config.instance_id, stage, stage_svc
                )

        for data_test in _get_valid_stages_data():
            stage = data_test[0]
            with self.subTest(stage=stage):
                _run_sub_test(stage)

    def test_run_stage_out_of_order_with_dry_run(
        self,
    ) -> None:
        """
        tests that run_stage runs stage_svc out of order when dry run is passed
        """

        def _run_sub_test(stage: PrivateComputationBaseStageFlow):
            ################ STAGE OUT OF ORDER WITH DRY RUN #####################
            stage_svc = self._get_dummy_stage_svc()
            pl_instance = self.create_sample_instance(
                status=PrivateComputationInstanceStatus.UNKNOWN
            )

            self.private_computation_service.instance_repository.read = MagicMock(
                return_value=pl_instance
            )

            pl_instance = self.private_computation_service.run_stage(
                pl_instance.infra_config.instance_id, stage, stage_svc, dry_run=True
            )
            self.assertEqual(pl_instance.infra_config.status, stage.initialized_status)

        for data_test in _get_valid_stages_data():
            stage = data_test[0]
            with self.subTest(stage=stage):
                _run_sub_test(stage)

    def test_run_stage_out_of_order_without_dry_run(
        self,
    ) -> None:
        """
        tests that run_stage does not run stage_svc out of order when dry run is not passed
        """

        def _run_sub_test(stage: PrivateComputationBaseStageFlow):
            ####################### STAGE OUT OF ORDER NO DRY RUN ############################
            stage_svc = self._get_dummy_stage_svc()
            pl_instance = self.create_sample_instance(
                status=PrivateComputationInstanceStatus.UNKNOWN
            )

            self.private_computation_service.instance_repository.read = MagicMock(
                return_value=pl_instance
            )

            with self.assertRaises(ValueError):
                pl_instance = self.private_computation_service.run_stage(
                    pl_instance.infra_config.instance_id,
                    stage,
                    stage_svc,
                    dry_run=False,
                )

        for data_test in _get_valid_stages_data():
            stage = data_test[0]
            with self.subTest(stage=stage):
                _run_sub_test(stage)

    def test_run_stage_partner_no_server_ips(
        self,
    ) -> None:
        """
        if it's a joint stage (partner requires server ips) but partner doesn't provide server ips, value error is thrown.
        Otherwise, things run as they should.
        """

        def _run_sub_test(stage: PrivateComputationBaseStageFlow):
            ####################### PARTNER NO SERVER IPS ############################
            stage_svc = self._get_dummy_stage_svc()
            pl_instance = self.create_sample_instance(
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                status=stage.previous_stage.completed_status,
                role=PrivateComputationRole.PARTNER,
            )

            self.private_computation_service.instance_repository.read = MagicMock(
                return_value=pl_instance
            )

            if stage.is_joint_stage:
                with self.assertRaises(ValueError):
                    pl_instance = self.private_computation_service.run_stage(
                        pl_instance.infra_config.instance_id, stage, stage_svc
                    )
            else:
                pl_instance = self.private_computation_service.run_stage(
                    pl_instance.infra_config.instance_id, stage, stage_svc
                )
                self.assertEqual(
                    pl_instance.infra_config.status, stage.initialized_status
                )

        for data_test in _get_valid_stages_data():
            stage = data_test[0]
            with self.subTest(stage=stage):
                _run_sub_test(stage)

    def test_run_stage_fails(
        self,
    ) -> None:
        """
        tests that statuses are set properly when a run fails
        """

        def _run_sub_test(stage: PrivateComputationBaseStageFlow):
            ######################### STAGE FAILS ####################################
            stage_svc = self._get_dummy_stage_svc()
            pl_instance = self.create_sample_instance(
                # pyre-fixme[16]: `Optional` has no attribute `completed_status`.
                status=stage.previous_stage.completed_status
            )

            self.private_computation_service.instance_repository.read = MagicMock(
                return_value=pl_instance
            )

            # create a custom exception class to make sure we have a unique exception for the test
            stage_failure_exception = type(
                "TestStageFailureException", (Exception,), {}
            )
            stage_svc.run_async = AsyncMock(side_effect=stage_failure_exception())

            with self.assertRaises(stage_failure_exception):
                pl_instance = self.private_computation_service.run_stage(
                    pl_instance.infra_config.instance_id, stage, stage_svc
                )

            self.assertEqual(pl_instance.infra_config.status, stage.failed_status)

        for data_test in _get_valid_stages_data():
            stage = data_test[0]
            with self.subTest(stage=stage):
                _run_sub_test(stage)

    @patch("fbpcs.private_computation.service.mpc.mpc.MPCService")
    async def test_create_and_start_mpc_instance(self, mock_mpc_svc) -> None:
        mock_mpc_svc.get_instance = MagicMock(side_effect=Exception())
        mock_mpc_svc.create_instance = MagicMock()
        mock_mpc_svc.start_instance_async = AsyncMock()

        instance_id = "test_instance_id"
        game_name = GameNames.LIFT.value
        mpc_party = MPCParty.CLIENT
        num_containers = 4
        input_file = "input_file"
        output_file = "output_file"
        input_directory = "input_directory"
        output_directory = "output_directory"
        server_ips = ["192.0.2.0", "192.0.2.1"]
        game_args = [
            {
                "input_filenames": input_file,
                "input_directory": input_directory,
                "output_filenames": output_file,
                "output_directory": output_directory,
                "concurrency": 1,
            }
        ]
        binary_version = self.onedocker_binary_config_map[
            OneDockerBinaryNames.LIFT_COMPUTE.value
        ].binary_version

        await create_and_start_mpc_instance(
            mpc_svc=mock_mpc_svc,
            instance_id=instance_id,
            game_name=game_name,
            mpc_party=mpc_party,
            num_containers=num_containers,
            binary_version=binary_version,
            server_certificate_provider=NullCertificateProvider(),
            ca_certificate_provider=NullCertificateProvider(),
            server_certificate_path="",
            ca_certificate_path="",
            container_timeout=DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
            server_ips=server_ips,
            game_args=game_args,
        )

        # check create_instance and start_instance were called with the right parameters
        self.assertEqual(
            call(
                instance_id=instance_id,
                game_name=game_name,
                mpc_party=mpc_party,
                num_workers=num_containers,
                game_args=game_args,
                server_uris=None,
            ),
            mock_mpc_svc.create_instance.call_args,
        )

        env_vars = {}
        self.assertEqual(
            call(
                instance_id=instance_id,
                server_ips=server_ips,
                timeout=DEFAULT_CONTAINER_TIMEOUT_IN_SEC,
                version=binary_version,
                env_vars=env_vars,
                certificate_request=None,
                wait_for_containers_to_start_up=True,
            ),
            mock_mpc_svc.start_instance_async.call_args,
        )

    def test_map_private_computation_role_to_mpc_party(self) -> None:
        self.assertEqual(
            MPCParty.SERVER,
            map_private_computation_role_to_mpc_party(PrivateComputationRole.PUBLISHER),
        )
        self.assertEqual(
            MPCParty.CLIENT,
            map_private_computation_role_to_mpc_party(PrivateComputationRole.PARTNER),
        )

    def test_get_status_from_stage(self) -> None:
        # Test get status from an MPC stage
        mpc_instance = PCSMPCInstance.create_instance(
            instance_id="test_mpc_id",
            game_name=GameNames.SHARD_AGGREGATOR.value,
            mpc_party=MPCParty.SERVER,
            num_workers=2,
            status=MPCInstanceStatus.FAILED,
        )
        pc_instance = self.create_sample_instance(
            PrivateComputationInstanceStatus.AGGREGATION_STARTED,
            instances=[mpc_instance],
        )
        self.private_computation_service.mpc_svc.update_instance = MagicMock(
            return_value=mpc_instance
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.AGGREGATION_FAILED,
            self.private_computation_service._update_instance(
                pc_instance
            ).infra_config.status,
        )

    def test_validate_metrics_results_doesnt_match(self) -> None:
        self.private_computation_service.storage_svc.read = MagicMock()
        # pyre-fixme[16]: Callable `read` has no attribute `side_effect`.
        self.private_computation_service.storage_svc.read.side_effect = [
            '{"subGroupMetrics":[],"metrics":{"controlClicks":1,"testSpend":0,"controlImpressions":0,"testImpressions":0,"controlMatchCount":0,"testMatchCount":0,"controlNumConvSquared":0,"testNumConvSquared":0,"testValueSquared":0,"controlValue":0,"testValue":0,"testConverters":0,"testConversions":0,"testPopulation":0,"controlClickers":0,"testClickers":0,"controlReach":0,"testReach":0,"controlSpend":0,"testClicks":0,"controlValueSquared":0,"controlConverters":0,"controlConversions":0,"controlPopulation":0}}',
            '{"subGroupMetrics":[],"metrics":{"testSpend":0,"controlClicks":0,"controlImpressions":0,"testImpressions":0,"controlMatchCount":0,"testMatchCount":0,"controlNumConvSquared":0,"testNumConvSquared":0,"testValueSquared":0,"controlValue":0,"testValue":0,"testConverters":0,"testConversions":0,"testPopulation":0,"controlClickers":0,"testClickers":0,"controlReach":0,"testReach":0,"controlSpend":0,"testClicks":0,"controlValueSquared":0,"controlConverters":0,"controlConversions":0,"controlPopulation":0}}',
        ]
        with self.assertRaises(PrivateComputationServiceValidationError):
            self.private_computation_service.validate_metrics(
                instance_id="test_id",
                aggregated_result_path="aggregated_result_path",
                expected_result_path="expected_result_path",
            )

    def test_validate_pid_dfca_csv_match(self) -> None:
        self.test_num_containers = 1

        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            role=PrivateComputationRole.PUBLISHER,
            game_type=PrivateComputationGameType.PRIVATE_ID_DFCA,
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        self.private_computation_service.storage_svc.read = MagicMock()
        # pyre-fixme[16]: Callable `read` has no attribute `side_effect`.
        self.private_computation_service.storage_svc.read.side_effect = [
            "publisher_user_id,partner_user_id\nc0f2421d-ea40-4f89-a489-44b824de8e14,4e89ad4a-245f-4a92-b3af-535d927fd1d4\n7a317fb7-e92d-4c29-882e-adad32c31000,283d0555-d2dd-4558-9775-b9a99353b560\n897a7026-af23-4994-8e37-cf807648b288,8345ea97-df26-499a-9d11-a99249eb0182\nd7bf6641-5004-4337-909c-19c7c65efc06,48cbaeea-ad08-4fbc-ad08-0e7d963715b1\nfde8494b-b02b-4946-b33c-87f7cc2f5e58,37b8907c-a4b9-4556-9e9e-efc0cfbaa697\n652ef7e5-d827-4366-8790-0c8137cd9079,a9511c9c-e643-49d3-b2c5-bca45509efdd\nbed88de1-7c5c-4e4e-9570-c0096f54d722,880e7c7f-91cd-48a4-ae6f-f9ea92ee0132\n7fadac6d-a795-4923-b4b5-3f54e0d2c7b2,00a8f8c3-25bf-4e6d-8997-811afe97e1cb\n187ffc5c-d0d6-4350-9d44-b5b51f091f97,2da9c08c-585c-4eba-b8df-15f5da1458b5\n65e7cc16-bb59-4c87-9598-5d4d47d5e11f,72b2f924-c92b-430f-b223-878672301d5b\na2f08fc4-a637-49ed-99c1-6fba7906970c,d6df91fa-6ef9-4466-aa5d-acd3042f811f\n89b5aed4-aa65-452e-9532-aaa164c06ad4,2f035d2a-48d2-4c57-8d7e-5b0fba788099\n4411390e-62cc-4f59-a7ae-a58a30306893,d91fcfe0-7a0d-4169-ba0d-5e3de3c468f2\nea199c58-3b81-4ec9-9d66-3754969bd46c,e7eb0fd6-5b54-491b-bede-0ed3d1f6a6cc\n64772612-5889-449c-8571-000fd8b0c9fb,7ce0e7b2-6a20-4acc-95a7-d9a6cd48c7f2",
            "publisher_user_id,partner_user_id\n7a317fb7-e92d-4c29-882e-adad32c31000,283d0555-d2dd-4558-9775-b9a99353b560\nc0f2421d-ea40-4f89-a489-44b824de8e14,4e89ad4a-245f-4a92-b3af-535d927fd1d4\n897a7026-af23-4994-8e37-cf807648b288,8345ea97-df26-499a-9d11-a99249eb0182\nd7bf6641-5004-4337-909c-19c7c65efc06,48cbaeea-ad08-4fbc-ad08-0e7d963715b1\nfde8494b-b02b-4946-b33c-87f7cc2f5e58,37b8907c-a4b9-4556-9e9e-efc0cfbaa697\n652ef7e5-d827-4366-8790-0c8137cd9079,a9511c9c-e643-49d3-b2c5-bca45509efdd\nbed88de1-7c5c-4e4e-9570-c0096f54d722,880e7c7f-91cd-48a4-ae6f-f9ea92ee0132\n7fadac6d-a795-4923-b4b5-3f54e0d2c7b2,00a8f8c3-25bf-4e6d-8997-811afe97e1cb\n187ffc5c-d0d6-4350-9d44-b5b51f091f97,2da9c08c-585c-4eba-b8df-15f5da1458b5\n65e7cc16-bb59-4c87-9598-5d4d47d5e11f,72b2f924-c92b-430f-b223-878672301d5b\na2f08fc4-a637-49ed-99c1-6fba7906970c,d6df91fa-6ef9-4466-aa5d-acd3042f811f\n89b5aed4-aa65-452e-9532-aaa164c06ad4,2f035d2a-48d2-4c57-8d7e-5b0fba788099\n4411390e-62cc-4f59-a7ae-a58a30306893,d91fcfe0-7a0d-4169-ba0d-5e3de3c468f2\nea199c58-3b81-4ec9-9d66-3754969bd46c,e7eb0fd6-5b54-491b-bede-0ed3d1f6a6cc\n64772612-5889-449c-8571-000fd8b0c9fb,7ce0e7b2-6a20-4acc-95a7-d9a6cd48c7f2",
        ]

        self.private_computation_service.validate_metrics(
            instance_id=self.test_private_computation_id,
            aggregated_result_path="aggregated_result_path",
            expected_result_path="expected_result_path",
        )

    def test_validate_pid_dfca_csv_no_match(self) -> None:
        self.test_num_containers = 1

        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            role=PrivateComputationRole.PUBLISHER,
            game_type=PrivateComputationGameType.PRIVATE_ID_DFCA,
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        self.private_computation_service.storage_svc.read = MagicMock()
        # pyre-fixme[16]: Callable `read` has no attribute `side_effect`.
        self.private_computation_service.storage_svc.read.side_effect = [
            "publisher_user_id,partner_user_id\nd0f2421d-ea40-4f89-a489-44b824de8e14,4e89ad4a-245f-4a92-b3af-535d927fd1d4\n7a317fb7-e92d-4c29-882e-adad32c31000,283d0555-d2dd-4558-9775-b9a99353b560\n897a7026-af23-4994-8e37-cf807648b288,8345ea97-df26-499a-9d11-a99249eb0182\nd7bf6641-5004-4337-909c-19c7c65efc06,48cbaeea-ad08-4fbc-ad08-0e7d963715b1\nfde8494b-b02b-4946-b33c-87f7cc2f5e58,37b8907c-a4b9-4556-9e9e-efc0cfbaa697\n652ef7e5-d827-4366-8790-0c8137cd9079,a9511c9c-e643-49d3-b2c5-bca45509efdd\nbed88de1-7c5c-4e4e-9570-c0096f54d722,880e7c7f-91cd-48a4-ae6f-f9ea92ee0132\n7fadac6d-a795-4923-b4b5-3f54e0d2c7b2,00a8f8c3-25bf-4e6d-8997-811afe97e1cb\n187ffc5c-d0d6-4350-9d44-b5b51f091f97,2da9c08c-585c-4eba-b8df-15f5da1458b5\n65e7cc16-bb59-4c87-9598-5d4d47d5e11f,72b2f924-c92b-430f-b223-878672301d5b\na2f08fc4-a637-49ed-99c1-6fba7906970c,d6df91fa-6ef9-4466-aa5d-acd3042f811f\n89b5aed4-aa65-452e-9532-aaa164c06ad4,2f035d2a-48d2-4c57-8d7e-5b0fba788099\n4411390e-62cc-4f59-a7ae-a58a30306893,d91fcfe0-7a0d-4169-ba0d-5e3de3c468f2\nea199c58-3b81-4ec9-9d66-3754969bd46c,e7eb0fd6-5b54-491b-bede-0ed3d1f6a6cc\n64772612-5889-449c-8571-000fd8b0c9fb,7ce0e7b2-6a20-4acc-95a7-d9a6cd48c7f2",
            "publisher_user_id,partner_user_id\n7a317fb7-e92d-4c29-882e-adad32c31000,283d0555-d2dd-4558-9775-b9a99353b560\nc0f2421d-ea40-4f89-a489-44b824de8e14,4e89ad4a-245f-4a92-b3af-535d927fd1d4\n897a7026-af23-4994-8e37-cf807648b288,8345ea97-df26-499a-9d11-a99249eb0182\nd7bf6641-5004-4337-909c-19c7c65efc06,48cbaeea-ad08-4fbc-ad08-0e7d963715b1\nfde8494b-b02b-4946-b33c-87f7cc2f5e58,37b8907c-a4b9-4556-9e9e-efc0cfbaa697\n652ef7e5-d827-4366-8790-0c8137cd9079,a9511c9c-e643-49d3-b2c5-bca45509efdd\nbed88de1-7c5c-4e4e-9570-c0096f54d722,880e7c7f-91cd-48a4-ae6f-f9ea92ee0132\n7fadac6d-a795-4923-b4b5-3f54e0d2c7b2,00a8f8c3-25bf-4e6d-8997-811afe97e1cb\n187ffc5c-d0d6-4350-9d44-b5b51f091f97,2da9c08c-585c-4eba-b8df-15f5da1458b5\n65e7cc16-bb59-4c87-9598-5d4d47d5e11f,72b2f924-c92b-430f-b223-878672301d5b\na2f08fc4-a637-49ed-99c1-6fba7906970c,d6df91fa-6ef9-4466-aa5d-acd3042f811f\n89b5aed4-aa65-452e-9532-aaa164c06ad4,2f035d2a-48d2-4c57-8d7e-5b0fba788099\n4411390e-62cc-4f59-a7ae-a58a30306893,d91fcfe0-7a0d-4169-ba0d-5e3de3c468f2\nea199c58-3b81-4ec9-9d66-3754969bd46c,e7eb0fd6-5b54-491b-bede-0ed3d1f6a6cc\n64772612-5889-449c-8571-000fd8b0c9fb,7ce0e7b2-6a20-4acc-95a7-d9a6cd48c7f2",
        ]

        with self.assertRaises(PrivateComputationServiceValidationError):
            self.private_computation_service.validate_metrics(
                instance_id=self.test_private_computation_id,
                aggregated_result_path="aggregated_result_path",
                expected_result_path="expected_result_path",
            )

    def test_cancel_current_stage(self) -> None:
        test_mpc_id = self.test_private_computation_id + "_compute_metrics"
        test_game_name = GameNames.LIFT.value
        test_mpc_party = MPCParty.CLIENT

        # prepare the pl instance that will be read in to memory from the repository
        # at the beginning of the cancel_current_stage function
        mpc_instance_started = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=test_mpc_party,
            num_workers=self.test_num_containers,
            status=MPCInstanceStatus.STARTED,
        )
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.COMPUTATION_STARTED,
            role=PrivateComputationRole.PARTNER,
            instances=[mpc_instance_started],
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        # prepare the mpc instance that's returned from mpc_service.stop_instance()
        mpc_instance_canceled = PCSMPCInstance.create_instance(
            instance_id=test_mpc_id,
            game_name=test_game_name,
            mpc_party=test_mpc_party,
            num_workers=self.test_num_containers,
            status=MPCInstanceStatus.CANCELED,
        )
        self.private_computation_service.mpc_svc.stop_instance = MagicMock(
            return_value=mpc_instance_canceled
        )
        self.private_computation_service.mpc_svc.instance_repository.read = MagicMock(
            return_value=mpc_instance_canceled
        )

        # call cancel, expect no exception
        private_computation_instance = (
            self.private_computation_service.cancel_current_stage(
                instance_id=self.test_private_computation_id,
            )
        )

        # assert the pl instance returned has the correct status
        self.assertEqual(
            PrivateComputationInstanceStatus.COMPUTATION_FAILED,
            private_computation_instance.infra_config.status,
        )

    def test_get_default_pid_stage_service(self) -> None:
        """
        Test for get_default_stage_service method in stage flow classes
        """
        args = self.private_computation_service.stage_service_args
        actual_service = (
            PrivateComputationStageFlow.PID_SHARD.get_default_stage_service(args)
        )
        self.assertIsInstance(actual_service, PIDShardStageService)

        actual_service = (
            PrivateComputationStageFlow.PID_PREPARE.get_default_stage_service(args)
        )
        self.assertIsInstance(actual_service, PIDPrepareStageService)

        actual_service = PrivateComputationStageFlow.ID_MATCH.get_default_stage_service(
            args
        )
        self.assertIsInstance(actual_service, PIDRunProtocolStageService)

    def test_get_default_stage_service_error(self) -> None:
        """
        Test for get_default_stage_service method in stage flow classes
        """
        args = self.private_computation_service.stage_service_args

        with self.assertRaises(NotImplementedError):
            DummyStageFlow.STAGE_1.get_default_stage_service(args)

    def test_get_stage_service(self) -> None:
        """
        Test for get_stage_service method in stage flow classes
        """
        args = self.private_computation_service.stage_service_args
        actual_service = (
            PrivateComputationPCF2StageFlow.PCF2_ATTRIBUTION.get_stage_service(args)
        )

        self.assertIsInstance(actual_service, PCF2AttributionStageService)
        # We need this line so pyre knows
        assert isinstance(actual_service, PCF2AttributionStageService)

        self.assertEqual(actual_service._mpc_service, args.mpc_svc)
        self.assertEqual(
            actual_service._log_cost_to_s3,
            DEFAULT_LOG_COST_TO_S3,
        )
        self.assertEqual(
            actual_service._onedocker_binary_config_map,
            args.onedocker_binary_config_map,
        )

    @patch("fbpcs.experimental.cloud_logs.dummy_log_retriever.DummyLogRetriever.fetch")
    def test_log_failed_containers(self, mock_log_fetch) -> None:
        for log_only_first_failure in (True, False):
            for num_failures in (0, 1, 2):
                with self.subTest(
                    log_only_first_failure=log_only_first_failure,
                    num_failures=num_failures,
                ):
                    mock_log_fetch.reset_mock()
                    state_instance = StageStateInstance(
                        instance_id=self.test_private_computation_id,
                        stage_name="test_stage",
                        containers=[
                            ContainerInstance(
                                instance_id="id", status=ContainerInstanceStatus.STARTED
                            )
                        ]
                        + [
                            ContainerInstance(
                                instance_id="id", status=ContainerInstanceStatus.FAILED
                            )
                            for _ in range(num_failures)
                        ],
                    )
                    private_computation_instance = self.create_sample_instance(
                        status=PrivateComputationInstanceStatus.PID_SHARD_FAILED,
                        role=PrivateComputationRole.PARTNER,
                        instances=[state_instance],
                    )
                    self.private_computation_service.instance_repository.read = (
                        MagicMock(return_value=private_computation_instance)
                    )

                    self.private_computation_service.log_failed_containers(
                        self.test_private_computation_id,
                        log_only_first_failure=log_only_first_failure,
                    )
                    if num_failures == 0:
                        self.assertEqual(mock_log_fetch.call_count, 0)
                    elif log_only_first_failure:
                        self.assertEqual(mock_log_fetch.call_count, 1)
                    else:
                        self.assertEqual(mock_log_fetch.call_count, num_failures)

    @mock.patch.object(
        PrivateComputationStageFlow,
        "get_stage_service",
    )
    def test_cancel_current_stage_state(self, mock_get_stage_service) -> None:
        mock_stage_svc = Mock(spec=self._get_dummy_stage_svc())
        mock_stage_svc.get_status.return_value = (
            PrivateComputationInstanceStatus.CREATION_FAILED
        )
        mock_get_stage_service.return_value = mock_stage_svc
        # create one StageStateInstance to be put into PrivateComputationInstance
        # at the beginning of the cancel_current_stage function
        state_instance = StageStateInstance(
            instance_id=self.test_private_computation_id,
            stage_name="test_stage",
        )
        private_computation_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.CREATION_STARTED,
            role=PrivateComputationRole.PARTNER,
            instances=[state_instance],
        )
        self.private_computation_service.instance_repository.read = MagicMock(
            return_value=private_computation_instance
        )

        # call cancel, expect no exception
        private_computation_instance = (
            self.private_computation_service.cancel_current_stage(
                instance_id=self.test_private_computation_id,
            )
        )

        # aseerts
        mock_stage_svc.stop_service.assert_called_once_with(
            private_computation_instance
        )
        self.assertEqual(
            PrivateComputationInstanceStatus.CREATION_FAILED,
            private_computation_instance.infra_config.status,
        )

    def test_server_ips(self) -> None:
        # empty case
        empty_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.CREATED
        )
        self.assertEqual(empty_instance.server_ips, [])

        # non empty case
        stage_state_instance = StageStateInstance(
            instance_id=self.test_private_computation_id,
            stage_name="test_stage",
            status=StageStateInstanceStatus.COMPLETED,
            containers=[
                ContainerInstance(
                    instance_id="test_container_instance_0",
                    ip_address="1.1.1.1",
                    status=ContainerInstanceStatus.COMPLETED,
                )
            ],
        )
        non_empty_instance = self.create_sample_instance(
            status=PrivateComputationInstanceStatus.CREATED,
            instances=[stage_state_instance],
        )
        self.assertEqual(non_empty_instance.server_ips, ["1.1.1.1"])

    def test_server_uris(self) -> None:
        test_uris = ["study123.pci.facebook.com"]

        stage_state_instance = StageStateInstance(
            instance_id=self.test_private_computation_id,
            stage_name="test_stage",
            status=StageStateInstanceStatus.COMPLETED,
            containers=[
                ContainerInstance(
                    instance_id="test_container_instance_0",
                    ip_address="1.1.1.1",
                    status=ContainerInstanceStatus.COMPLETED,
                )
            ],
            server_uris=test_uris,
        )

        self.assertEqual(stage_state_instance.server_uris, test_uris)

    def test_fbpcs_bundle_id(self) -> None:
        TEST_BUNDLE_ID = str(random.randint(100, 200))
        with patch.dict(os.environ, {FBPCS_BUNDLE_ID: TEST_BUNDLE_ID}):
            test_instance = self.create_sample_instance(
                status=PrivateComputationInstanceStatus.CREATED
            )
            self.assertEqual(test_instance.infra_config.fbpcs_bundle_id, TEST_BUNDLE_ID)

    def create_sample_instance(
        self,
        status: PrivateComputationInstanceStatus,
        role: PrivateComputationRole = PrivateComputationRole.PUBLISHER,
        instances: Optional[List[UnionedPCInstance]] = None,
        game_type: PrivateComputationGameType = PrivateComputationGameType.LIFT,
        status_updates: Optional[List[StatusUpdate]] = None,
    ) -> PrivateComputationInstance:
        infra_config: InfraConfig = InfraConfig(
            instance_id=self.test_private_computation_id,
            role=role,
            status=status,
            status_update_ts=1600000000,
            instances=instances or [],
            game_type=game_type,
            num_pid_containers=self.test_num_containers,
            num_mpc_containers=self.test_num_containers,
            num_files_per_mpc_container=NUM_NEW_SHARDS_PER_FILE,
            mpc_compute_concurrency=self.test_concurrency,
            status_updates=status_updates or [],
            log_cost_bucket=self.log_cost_bucket,
            server_certificate=SAMPLE_SERVER_CERTIFICATE,
            ca_certificate=SAMPLE_CA_CERTIFICATE,
        )
        common: CommonProductConfig = CommonProductConfig(
            input_path=self.test_input_path,
            output_dir=self.test_output_dir,
            hmac_key=self.test_hmac_key,
        )
        product_config: ProductConfig = LiftConfig(
            common=common,
            k_anonymity_threshold=DEFAULT_K_ANONYMITY_THRESHOLD_PL,
        )
        return PrivateComputationInstance(
            infra_config=infra_config,
            product_config=product_config,
        )


class TestTransformFilePath(unittest.TestCase):
    def test_virtual_hosted_format(self) -> None:

        test_cases = [
            "https://bucket-name.s3.Region.amazonaws.com/key-name",
            "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/results/partner_expected_result.json",
            "https://s3-s3-amazonaws-com-name.s3.Region.amazonaws.com/us-west-s3S3amazoncom/-name",  # contrived, 'worst' case example
            "https://bucket-name-more-dashes.s3.us-east-1.amazonaws.com/Capital!(/LETTERS/06/12/976e100-75ig-4fjfjfcee-aaaa3l-f36a9e258.csv",
        ]
        expected_results = [
            "https://bucket-name.s3.Region.amazonaws.com/key-name",
            "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/results/partner_expected_result.json",
            "https://s3-s3-amazonaws-com-name.s3.Region.amazonaws.com/us-west-s3S3amazoncom/-name",
            "https://bucket-name-more-dashes.s3.us-east-1.amazonaws.com/Capital!(/LETTERS/06/12/976e100-75ig-4fjfjfcee-aaaa3l-f36a9e258.csv",
        ]

        for x, y in zip(test_cases, expected_results):
            self.assertEqual(transform_file_path(x), y)

    def test_s3_format(self) -> None:

        test_cases = [
            "S3://bucket-name/key-name",
            "s3://bucket-name/key-name",
            "s3://fbpcs-github-e2e/lift/results/partner_expected_result.json",
            "s3://fbpcs-github-e2e/lift/results/Uppercase!(/partner_expected_result.json",
        ]
        expected_results = [
            "https://bucket-name.s3.Region.amazonaws.com/key-name",
            "https://bucket-name.s3.Region.amazonaws.com/key-name",
            "https://fbpcs-github-e2e.s3.Region.amazonaws.com/lift/results/partner_expected_result.json",
            "https://fbpcs-github-e2e.s3.Region.amazonaws.com/lift/results/Uppercase!(/partner_expected_result.json",
        ]

        for x, y in zip(test_cases, expected_results):
            self.assertEqual(transform_file_path(x, "Region"), y)

    def test_path_format(self) -> None:

        test_cases = [
            "https://s3.Region.amazonaws.com/bucket-name/key-name",
            "https://s3.us-west-2.amazonaws.com/fbpcs-github-e2e/lift/results/partner_expected_result.json",
            "https://s3.us-west-2.amazonaws.com/fbpcs-github-e2e/lift/results/Uppercase!(/partner_expected_result.json",
        ]
        expected_results = [
            "https://bucket-name.s3.Region.amazonaws.com/key-name",
            "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/results/partner_expected_result.json",
            "https://fbpcs-github-e2e.s3.us-west-2.amazonaws.com/lift/results/Uppercase!(/partner_expected_result.json",
        ]

        for x, y in zip(test_cases, expected_results):
            self.assertEqual(transform_file_path(x), y)

    def test_bad_inputs(self) -> None:

        test_cases = [
            "",
            "www.facebook.com",
            "aaaa",
        ]

        for x in test_cases:
            with self.assertRaises(ValueError):
                transform_file_path(x)
