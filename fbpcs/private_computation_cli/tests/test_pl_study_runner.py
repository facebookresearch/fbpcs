#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

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

from fbpcs.pl_coordinator.exceptions import PCStudyValidationException
from fbpcs.private_computation.stage_flows.private_computation_stage_flow import (
    PrivateComputationStageFlow,
)

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S+0000"

PCGraphAPIClientMock = MagicMock()


@patch("fbpcs.pl_coordinator.pl_study_runner.PCGraphAPIClient", PCGraphAPIClientMock)
class TestPlStudyRunner(TestCase):
    TEST_STUDY_ID = "test_study_1"
    TEST_OBJECTIVE_ID_1 = "OBJECTIVE1"
    TEST_OBJECTIVE_ID_2 = "OBJECTIVE2"
    TEST_OBJECTIVE_IDS = [TEST_OBJECTIVE_ID_1, TEST_OBJECTIVE_ID_2]
    TEST_INPUT_PATHS = ["input/path/1", "input/path/2"]
    TEST_STAGE_FLOW = PrivateComputationStageFlow

    @patch("logging.Logger")
    @patch("fbpcs.pl_coordinator.pc_graphapi_utils.PCGraphAPIClient")
    def setUp(
        self,
        mock_graph_api_client,
        mock_logger,
    ) -> None:
        self.config = {}
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
        PCGraphAPIClientMock.return_value = self.client_mock
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
        tested_features = pl_study_runner._get_pcs_features(
            self.cell_obj_instances, self.mock_graph_api_client
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

    def _get_graph_api_output(
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
