#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import asyncio
import calendar
import copy
import datetime
import json
import logging
import random
import time
from typing import Any, List
from unittest import TestCase
from unittest.mock import MagicMock, patch, PropertyMock

import requests
from fbpcs.pl_coordinator import pl_study_runner
from fbpcs.pl_coordinator.exceptions import (
    GraphAPIGenericException,
    OneCommandRunnerExitCode,
    PCStudyValidationException,
)
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+0000"

BoltGraphAPIClientMock = MagicMock()


@patch(
    "fbpcs.pl_coordinator.pl_study_runner.BoltGraphAPIClient", BoltGraphAPIClientMock
)
class TestPlStudyRunner(TestCase):
    TEST_STUDY_ID = "test_study_1"
    TEST_OBJECTIVE_ID_1 = "OBJECTIVE1"
    TEST_OBJECTIVE_ID_2 = "OBJECTIVE2"
    TEST_OBJECTIVE_IDS = [TEST_OBJECTIVE_ID_1, TEST_OBJECTIVE_ID_2]
    TEST_INPUT_PATHS = ["input/path/1", "input/path/2"]
    TEST_STAGE_FLOW = PrivateComputationStageFlow

    @patch("logging.Logger")
    @patch("fbpcs.pl_coordinator.bolt_graphapi_client.BoltGraphAPIClient")
    def setUp(
        self,
        mock_graph_api_client,
        mock_logger,
    ) -> None:
        # this is the start of a valid private computation config.yml file
        self.config = {"private_computation": {"dependency": {}}}
        self.test_logger = logging.getLogger(__name__)
        self.client_mock = MagicMock()
        valid_start_date = datetime.datetime.now() - datetime.timedelta(hours=1)
        valid_observation_end_time = datetime.datetime.now() - datetime.timedelta(
            minutes=30
        )
        self.study_objective_data = [
            {
                "id": self.TEST_OBJECTIVE_ID_1,
                "type": "MPC_CONVERSION",
            },
            {
                "id": self.TEST_OBJECTIVE_ID_2,
                "type": "MPC_CONVERSION",
            },
        ]
        self.study_objectives = {"data": self.study_objective_data}
        self.opportunity_data_information = []
        self.study_data_dict = {
            "id": self.TEST_STUDY_ID,
            "type": "LIFT",
            "start_time": valid_start_date.isoformat(timespec="seconds") + "+0000",
            "observation_end_time": valid_observation_end_time.isoformat(
                timespec="seconds"
            )
            + "+0000",
            "objectives": self.study_objectives,
            "opp_data_information": self.opportunity_data_information,
        }
        self.response_mock = MagicMock()
        self.response_mock.text = json.dumps(self.study_data_dict)
        self.final_stage = PrivateComputationStageFlow.AGGREGATE

        self.mock_graph_api_client = mock_graph_api_client
        self.client_mock = MagicMock()
        self.client_mock.get_study_data.return_value = self.response_mock
        BoltGraphAPIClientMock.return_value = self.client_mock
        self.mock_logger = mock_logger
        self.num_shards = 2
        self.cell_id = str(random.randint(100, 200))
        self.objective_id = str(random.randint(100, 200))
        self.instance_id = str(random.randint(100, 200))
        self.cell_obj_instances = {
            self.cell_id: {
                self.objective_id: {
                    "latest_data_ts": 1645039053,
                    "input_path": "https://input/path/to/input.csv",
                    "num_shards": self.num_shards,
                    "instance_id": self.instance_id,
                    "status": "CREATED",
                }
            }
        }

    def _run_study(self, **kwargs) -> None:
        defaults = {
            "config": self.config,
            "study_id": self.TEST_STUDY_ID,
            "objective_ids": self.TEST_OBJECTIVE_IDS,
            "input_paths": self.TEST_INPUT_PATHS,
            "logger": self.test_logger,
            "stage_flow": self.TEST_STAGE_FLOW,
            "final_stage": self.final_stage,
        }
        defaults.update(**kwargs)
        pl_study_runner.run_study(**defaults)

    @patch("time.time", new=MagicMock(return_value=1665458111.3078792))
    def test_get_cell_obj_instance(self) -> None:
        for created_time, latest_data_ts, status, is_instance_valid in (
            # valid case
            (
                "2022-10-10T14:31:11+0000",
                1658355374,
                "PC_PRE_VALIDATION_COMPLETED",
                True,
            ),
            # invalid status case
            (
                "2022-10-10T14:31:11+0000",
                1658355374,
                "A_FAKE_STATUS",
                False,
            ),
            # expired instance case
            (
                "2022-10-05T14:31:11+0000",
                1658355374,
                "PC_PRE_VALIDATION_COMPLETED",
                False,
            ),
            # latest_data_ts > creation time case
            (
                "2022-10-10T14:31:11+0000",
                1665458000,
                "PC_PRE_VALIDATION_COMPLETED",
                False,
            ),
        ):
            with self.subTest(
                created_time=created_time,
                latest_data_ts=latest_data_ts,
                status=status,
                is_instance_valid=is_instance_valid,
            ):
                study_data = {
                    "type": "LIFT",
                    "start_time": "2022-10-03T07:00:00+0000",
                    "observation_end_time": "2023-04-01T07:00:00+0000",
                    "objectives": {
                        "data": [
                            {
                                "id": "11111111111111",
                                "name": "Objective",
                                "type": "MPC_CONVERSION",
                            }
                        ]
                    },
                    "opp_data_information": [
                        f'{{"breakdowns":{{"cell_id":22222222222222}},"latest_data_ts":{latest_data_ts},"num_shards":2,"data_file_path_url":"https:\\/\\/test-bucket.s3.us-west-2.amazonaws.com\\/lift\\/publisher\\/publisher_e2e_input.csv","hash_key":"0"}}'
                    ],
                    "instances": {
                        "data": [
                            {
                                "id": "33333333333333",
                                "breakdown_key": '{"cell_id":22222222222222,"objective_id":11111111111111}',
                                "status": status,
                                "server_ips": ["11.1.11.11"],
                                "tier": "private_measurement.private_computation_service_rc",
                                "feature_list": [
                                    "bolt_runner",
                                    "private_lift_pcf2_release",
                                ],
                                "created_time": created_time,
                                "latest_status_update_time": "2022-10-10T14:35:25+0000",
                            }
                        ]
                    },
                }
                objective_ids = ["11111111111111"]
                input_paths = [
                    "https://test-bucket.s3.us-west-2.amazonaws.com/lift/inputs/partner_e2e_input.csv"
                ]

                expected_results = {
                    "22222222222222": {
                        "11111111111111": {
                            "latest_data_ts": latest_data_ts,
                            "input_path": "https://test-bucket.s3.us-west-2.amazonaws.com/lift/inputs/partner_e2e_input.csv",
                            "num_shards": 2,
                        }
                    }
                }

                if is_instance_valid:
                    expected_results["22222222222222"]["11111111111111"].update(
                        {"instance_id": "33333333333333", "status": status}
                    )

                actual_results = pl_study_runner._get_cell_obj_instance(
                    study_data=study_data,
                    objective_ids=objective_ids,
                    input_paths=input_paths,
                )

                self.assertEqual(expected_results, actual_results)

    @patch("time.time", new=MagicMock(return_value=1665458111.3078792))
    def test_get_runnable_objectives(self) -> None:

        study_data = {
            "type": "LIFT",
            "start_time": "2022-10-03T07:00:00+0000",
            "observation_end_time": "2023-04-01T07:00:00+0000",
            "objectives": {
                "data": [
                    {
                        "id": "11111111111111",
                        "name": "Objective",
                        "type": "MPC_CONVERSION",
                    },
                    {
                        "id": "44444444444444",
                        "name": "Objective",
                        "type": "MPC_CONVERSION",
                    },
                    {
                        "id": "77777777777777",
                        "name": "Objective",
                        "type": "MPC_CONVERSION",
                    },
                ]
            },
            "opp_data_information": [
                '{"breakdowns":{"cell_id":22222222222222},"latest_data_ts":1658355374,"num_shards":2,"data_file_path_url":"https:\\/\\/test-bucket.s3.us-west-2.amazonaws.com\\/lift\\/publisher\\/publisher_e2e_input.csv","hash_key":"0"}'
            ],
            "instances": {
                "data": [
                    # ongoing run
                    {
                        "id": "33333333333333",
                        "breakdown_key": '{"cell_id":22222222222222,"objective_id":11111111111111}',
                        "status": "PC_PRE_VALIDATION_COMPLETED",
                        "server_ips": ["11.1.11.11"],
                        "tier": "private_measurement.private_computation_service_rc",
                        "feature_list": [
                            "bolt_runner",
                            "private_lift_pcf2_release",
                        ],
                        "created_time": "2022-10-10T14:31:11+0000",
                        "latest_status_update_time": "2022-10-10T14:35:25+0000",
                    },
                    # expired instance, so the objective id is runnable
                    {
                        "id": "55555555555555",
                        "breakdown_key": '{"cell_id":22222222222222,"objective_id":44444444444444}',
                        "status": "PC_PRE_VALIDATION_COMPLETED",
                        "server_ips": ["11.1.11.11"],
                        "tier": "private_measurement.private_computation_service_rc",
                        "feature_list": [
                            "bolt_runner",
                            "private_lift_pcf2_release",
                        ],
                        "created_time": "2022-10-07T14:31:11+0000",
                        "latest_status_update_time": "2022-10-07T14:35:25+0000",
                    },
                ]
            },
        }

        self.response_mock.text = json.dumps(study_data)
        self.client_mock.get_study_data.return_value = self.response_mock

        expected_results = ["44444444444444", "77777777777777"]

        actual_results = pl_study_runner.get_runnable_objectives(
            study_id=self.TEST_STUDY_ID,
            config=self.config,
            logger=self.test_logger,
        )

        self.assertEqual(expected_results, actual_results)

    def _validate_error(
        self, cause: str, remediation: str, logger_mock: MagicMock, **kwargs
    ) -> None:
        with self.assertRaises(SystemExit) as err_ctx:
            self._run_study(**kwargs)
        self.assertEqual(str(err_ctx.exception), "1")
        expected_exception = PCStudyValidationException(
            cause,
            remediation,
        )
        logger_mock.exception.assert_called_once()
        self.assertEqual(
            str(logger_mock.exception.call_args[0][0]), str(expected_exception)
        )

    def test_get_pcs_features(self) -> None:
        expected_features = ["dummy_feature1", "dummy_feature2"]
        self.mock_graph_api_client.get_instance.return_value = (
            self._get_graph_api_output("CREATED", expected_features)
        )
        tested_features = asyncio.run(
            pl_study_runner._get_pcs_features(
                self.cell_obj_instances, self.mock_graph_api_client
            )
        )
        self.assertEqual(tested_features, expected_features)

    @patch("fbpcs.pl_coordinator.exceptions.logging")
    def test_run_study_input_validation_errors(self, logger_mock) -> None:
        with self.subTest("duplicate_objective_ids"):
            try:
                invalid_objective_ids = ["1", "1"]
                self._validate_error(
                    "Error: objective_ids have duplicates",
                    "ensure objective_ids,input_paths have no duplicate and should be same 1-1 mapping",
                    logger_mock,
                    objective_ids=invalid_objective_ids,
                )
            finally:
                logger_mock.exception.reset_mock()

        with self.subTest("duplicate_input_paths"):
            try:
                invalid_input_paths = ["input/path/1", "input/path/1"]

                self._validate_error(
                    "Error: input_paths have duplicates",
                    "ensure objective_ids,input_paths have no duplicate and should be same 1-1 mapping",
                    logger_mock,
                    input_paths=invalid_input_paths,
                )
            finally:
                logger_mock.exception.reset_mock()

        with self.subTest("mismatch_objective_and_input_lengths"):
            try:
                invalid_input_paths = ["input/path/1"]
                self._validate_error(
                    "Error: Number of objective_ids and number of input_paths don't match.",
                    "ensure objective_ids,input_paths have no duplicate and should be same 1-1 mapping",
                    logger_mock,
                    input_paths=invalid_input_paths,
                )
            finally:
                logger_mock.exception.reset_mock()

        with self.subTest("multiple_errors"):
            try:
                invalid_input_paths = ["input/path/1", "input/path/1"]
                invalid_objective_ids = invalid_objective_ids = ["1", "1", "1"]
                self._validate_error(
                    "Error: objective_ids have duplicates\nError: input_paths have duplicates\nError: Number of objective_ids and number of input_paths don't match.",
                    "ensure objective_ids,input_paths have no duplicate and should be same 1-1 mapping",
                    logger_mock,
                    objective_ids=invalid_objective_ids,
                    input_paths=invalid_input_paths,
                )
            finally:
                logger_mock.exception.reset_mock()

    @patch("fbpcs.pl_coordinator.exceptions.logging")
    @patch("fbpcs.pl_coordinator.pl_study_runner.time")
    def test_run_study_study_type_validation_error(
        self, time_mock: MagicMock, logger_mock: MagicMock
    ) -> None:
        time_mock.strptime = time.strptime
        invalid_study_data_dict = dict(**self.study_data_dict)
        frozen_time = time.time()
        time_mock.time.return_value = frozen_time

        with self.subTest("token_unable_read_study"):
            try:
                self.client_mock.get_study_data.side_effect = GraphAPIGenericException(
                    "unable_read_study"
                )
                with self.assertRaises(SystemExit) as err_ctx:
                    self._run_study()

                self.assertEqual(
                    str(err_ctx.exception),
                    str(OneCommandRunnerExitCode.ERROR_READ_STUDY.value),
                )
            finally:
                self.client_mock.get_study_data.side_effect = None
                self.client_mock.reset_mock()
                logger_mock.exception.reset_mock()

        with self.subTest("invalid_type"):
            try:
                invalid_study_data_dict["type"] = "wrong"
                self.response_mock.text = json.dumps(invalid_study_data_dict)
                self.client_mock.get_study_data.return_value = self.response_mock
                self._validate_error(
                    "Error: Expected study type: LIFT. Study type: wrong.",
                    f"ensure {self.TEST_STUDY_ID} study is LIFT, must have started, finished less than 90 days ago.",
                    logger_mock,
                )
            finally:
                logger_mock.exception.reset_mock()
                invalid_study_data_dict["type"] = self.study_data_dict["type"]

        with self.subTest("invalid_study_start_time"):
            try:
                invalid_start_time = datetime.datetime.now() + datetime.timedelta(
                    hours=30
                )
                invalid_study_data_dict["start_time"] = (
                    invalid_start_time.isoformat(timespec="seconds") + "+0000"
                )
                self.response_mock.text = json.dumps(invalid_study_data_dict)
                self.client_mock.get_study_data.return_value = self.response_mock
                self._validate_error(
                    f"Error: Study must have started. Study start time: {calendar.timegm(time.strptime(invalid_start_time.isoformat(timespec='seconds') + '+0000', TIME_FORMAT))}. Current time: {int(frozen_time)}.",
                    f"ensure {self.TEST_STUDY_ID} study is LIFT, must have started, finished less than 90 days ago.",
                    logger_mock,
                )
            finally:
                logger_mock.exception.reset_mock()
                invalid_study_data_dict["start_time"] = self.study_data_dict[
                    "start_time"
                ]

        with self.subTest("invalid_observation_end_time"):
            try:
                invalid_observation_end_time = (
                    datetime.datetime.now() - datetime.timedelta(days=365)
                )
                invalid_study_data_dict["observation_end_time"] = (
                    invalid_observation_end_time.isoformat(timespec="seconds") + "+0000"
                )
                self.response_mock.text = json.dumps(invalid_study_data_dict)
                self.client_mock.get_study_data.return_value = self.response_mock
                self._validate_error(
                    "Error: Cannot run for study that finished more than 90 days ago.",
                    f"ensure {self.TEST_STUDY_ID} study is LIFT, must have started, finished less than 90 days ago.",
                    logger_mock,
                )
            finally:
                logger_mock.exception.reset_mock()
                invalid_study_data_dict["observation_end_time"] = self.study_data_dict[
                    "observation_end_time"
                ]

    @patch("fbpcs.pl_coordinator.exceptions.logging")
    def test_run_study_mpc_validation_errors(self, logger_mock) -> None:
        invalid_study_data_dict = copy.deepcopy(self.study_data_dict)
        with self.subTest("no_mpc_objectives"):
            try:
                invalid_study_data_dict["objectives"]["data"] = [
                    {"id": "invalid_id", "type": "invalid_type"}
                ]
                self.response_mock.text = json.dumps(invalid_study_data_dict)
                self.client_mock.get_study_data.return_value = self.response_mock
                self._validate_error(
                    f"Study {self.TEST_STUDY_ID} has no MPC objectives",
                    "check study data that need to have MPC objectives",
                    logger_mock,
                )
            finally:
                print(f"Study Objectives: {self.study_objectives}")
                invalid_study_data_dict["objectives"] = self.study_objectives
                logger_mock.exception.reset_mock()

        with self.subTest("invalid_objective_id"):
            try:
                print(invalid_study_data_dict)
                invalid_study_data_dict["objectives"]["data"][0][
                    "id"
                ] = "invalid_objective_id"
                self.response_mock.text = json.dumps(invalid_study_data_dict)
                self.client_mock.get_study_data.return_value = self.response_mock
                self._validate_error(
                    f"Objective id {self.TEST_OBJECTIVE_ID_1} invalid. Valid MPC objective ids for study {self.TEST_STUDY_ID}: {','.join(['invalid_objective_id', self.TEST_OBJECTIVE_ID_2])}",
                    "input objs are MPC objs of this study.",
                    logger_mock,
                )
            finally:
                invalid_study_data_dict["objectives"] = self.study_objectives
                logger_mock.exception.reset_mock()

        with self.subTest("cannot_read_ads_pixel"):
            try:
                invalid_study_data_dict["objectives"]["data"][0]["adspixels"] = {
                    "data": [{"id": "adspixel_id"}]
                }
                self.response_mock.text = json.dumps(invalid_study_data_dict)
                self.client_mock.get_study_data.return_value = self.response_mock
                self.client_mock.get_adspixels.side_effect = GraphAPIGenericException(
                    "unable_read_study"
                )
                with self.assertRaises(SystemExit) as err_ctx:
                    self._run_study()

                self.assertEqual(
                    str(err_ctx.exception),
                    str(OneCommandRunnerExitCode.ERROR_READ_ADSPIXELS.value),
                )
            finally:
                self.client_mock.get_adspixels.side_effect = None
                invalid_study_data_dict["objectives"] = self.study_objectives
                logger_mock.exception.reset_mock()

    @patch("fbpcs.pl_coordinator.exceptions.logging")
    def test_opp_data_information_validation_error(self, logger_mock) -> None:
        self.study_data_dict.pop("opp_data_information")
        self.response_mock.text = json.dumps(self.study_data_dict)
        self.client_mock.get_study_data.return_value = self.response_mock
        self._validate_error(
            f"Study {self.TEST_STUDY_ID} has no opportunity datasets.",
            f"Check {self.TEST_STUDY_ID} study data to include opp_data_information",
            logger_mock,
        )

    async def _get_graph_api_output(
        self, status: str, feature_list: List[str]
    ) -> requests.Response:
        data = {"status": status, "feature_list": feature_list}
        r = requests.Response()
        r.status_code = 200
        # pyre-ignore
        type(r).text = PropertyMock(return_value=json.dumps(data))

        def json_func(**kwargs) -> Any:
            return data

        r.json = json_func
        return r
