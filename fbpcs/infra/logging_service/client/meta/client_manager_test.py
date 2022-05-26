# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
CLI for ad-hoc testing against Logging Service server


Usage:
    client_manager_test.par <server_host> [options]

Options:
    -h --help                Show this help
"""

import logging

import schema
from docopt import docopt
from fbpcs.infra.logging_service.client.meta.client_manager import ClientManager
from fbpcs.infra.logging_service.client.meta.data_model.attribution_run_info import (
    AttributionRunInfo,
)
from fbpcs.infra.logging_service.client.meta.data_model.aws_log_location import (
    AwsLogLocation,
)
from fbpcs.infra.logging_service.client.meta.data_model.base_info import BaseInfo
from fbpcs.infra.logging_service.client.meta.data_model.computation_run_info import (
    ComputationRunInfo,
)
from fbpcs.infra.logging_service.client.meta.data_model.container_instance import (
    ContainerInstance,
)
from fbpcs.infra.logging_service.client.meta.data_model.lift_run_info import LiftRunInfo
from fbpcs.infra.logging_service.client.meta.data_model.log_location import LogLocation
from fbpcs.infra.logging_service.client.meta.data_model.study_stage_info import (
    StudyStageInfo,
)
from fbpcs.infra.logging_service.client.meta.utils import Utils


# Client for ad-hoc testing
def main() -> None:
    Utils.configure_logger("log/client_manager_test.log")
    logger = logging.getLogger(__name__)

    s = schema.Schema(
        {
            "<server_host>": str,
            "--help": bool,
        }
    )

    arguments = s.validate(docopt(__doc__))

    test_data_models()

    server_host = arguments["<server_host>"]
    server_port = Utils.get_server_port()
    logger.info(
        f'Getting ClientManager with server host: "{server_host}", and port: {server_port}'
    )
    client_manager = ClientManager(server_host, 9090)
    test_client_manager(client_manager)
    client_manager.close()

    logger.info("Getting ClientManager with no server, and every API should be no-op")
    client_manager = ClientManager("", 0)
    test_client_manager(client_manager)
    client_manager.close()


def test_client_manager(
    client_manager: ClientManager,
) -> None:
    logger = logging.getLogger(__name__)
    logger.info("test_client_manager. starting...")

    # send requests
    result = client_manager.put_metadata("partner1", "key2", "value2")
    logger.info(f"putMetadata: response: {result}.")

    entity_value = client_manager.get_metadata("partner1", "key2")
    logger.info(f"GetMetadata: entity_value: {entity_value}.")

    key_values = client_manager.list_metadata("partner1", "start1", "end1", 10)
    logger.info(f"ListMetadataRequest: key_values: {key_values}.")


def test_data_models() -> None:
    logger = logging.getLogger(__name__)
    logger.info("test_data_models. starting...")

    aws_log_location = AwsLogLocation("log group1", "log stream1")
    # pyre-ignore
    aws_log_location2 = AwsLogLocation.from_json(aws_log_location.to_json())
    logger.info(
        f"AwsLogLocation: json={aws_log_location.to_json()}, str={str(aws_log_location)}"
    )
    assert str(aws_log_location) == str(aws_log_location2), "json AwsLogLocation"

    log_location = LogLocation("note 1", "log url1", aws_log_location)
    # pyre-ignore
    log_location2 = LogLocation.from_json(log_location.to_json())
    logger.info(f"LogLocation: json={log_location.to_json()}, str={str(log_location)}")
    assert str(log_location) == str(log_location2), "json LogLocation"

    container_instance = ContainerInstance(
        "container ID1", "2022-04-15 19:00:46,099Z", log_location
    )
    # pyre-ignore
    container_instance2 = ContainerInstance.from_json(container_instance.to_json())
    logger.info(
        f"ContainerInstance: json={container_instance.to_json()}, str={str(container_instance)}"
    )
    assert str(container_instance) == str(container_instance2), "json ContainerInstance"

    computation_run_info = ComputationRunInfo(
        "v1",
        "ComputationRunInfo",
        "2022-04-15 19:00:46,099Z",
        "LIFT",
        "run_study",
    )
    # pyre-ignore
    computation_run_info2 = ComputationRunInfo.from_json(computation_run_info.to_json())
    logger.info(
        f"ComputationRunInfo: json={computation_run_info.to_json()}, str={str(computation_run_info)}"
    )
    assert str(computation_run_info) == str(
        computation_run_info2
    ), "json ComputationRunInfo"

    instance_objectives = {"instance1": "objective1", "instance2": "objective2"}
    lift_run_info = LiftRunInfo(
        "v1",
        "LiftRunInfo",
        "2022-04-15 19:00:46,099Z",
        "LIFT",
        "run_study",
        {"cell_id1": instance_objectives},
    )
    # pyre-ignore
    lift_run_info2 = LiftRunInfo.from_json(lift_run_info.to_json())
    logger.info(
        f"LiftRunInfo: json={lift_run_info.to_json()}, str={str(lift_run_info)}"
    )
    assert str(lift_run_info) == str(lift_run_info2), "json LiftRunInfo"

    base_info = BaseInfo("v1", "LiftRunInfo")
    # pyre-ignore
    base_info2 = BaseInfo.from_json(lift_run_info.to_json())
    logger.info(f"BaseInfo: json={base_info2.to_json()}, str={str(base_info2)}")
    assert str(base_info) == str(base_info2), "json BaseInfo"

    attribution_run_info = AttributionRunInfo(
        "v1",
        "AttributionRunInfo",
        "2022-04-15 19:00:46,099Z",
        "ATTRIBUTION",
        "attribution_run",
        "dataset id1",
    )
    # pyre-ignore
    attribution_run_info2 = AttributionRunInfo.from_json(attribution_run_info.to_json())
    logger.info(
        f"AttributionRunInfo: json={attribution_run_info.to_json()}, str={str(attribution_run_info)}"
    )
    assert str(attribution_run_info) == str(
        attribution_run_info2
    ), "json AttributionRunInfo"

    study_stage_info = StudyStageInfo(
        "v1",
        "StudyStageInfo",
        "run_id1",
        "instance id1",
        "2022-04-15 19:00:46,099Z",
        "stage instance type1",
        "stage name1",
        "stage status1",
        [container_instance, container_instance2],
    )
    # pyre-ignore
    study_stage_info2 = StudyStageInfo.from_json(study_stage_info.to_json())
    logger.info(
        f"StudyStageInfo: json={study_stage_info.to_json()}, str={str(study_stage_info)}"
    )
    assert str(study_stage_info) == str(study_stage_info2), "json StudyStageInfo"

    base_info = BaseInfo("v1", "StudyStageInfo")
    base_info2 = BaseInfo.from_json(study_stage_info.to_json())
    logger.info(f"BaseInfo: json={base_info2.to_json()}, str={str(base_info2)}")
    assert str(base_info) == str(base_info2), "json BaseInfo"


if __name__ == "__main__":
    main()
