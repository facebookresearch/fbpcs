#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging

from typing import Any, Dict, List, Tuple

from fbpcs.bolt.bolt_job import BoltJob, BoltPlayerArgs
from fbpcs.bolt.bolt_runner import BoltRunner
from fbpcs.bolt.constants import DEFAULT_POLL_INTERVAL_SEC
from fbpcs.bolt.oss_bolt_pcs import BoltPCSClient, BoltPCSCreateInstanceArgs
from fbpcs.private_computation_cli.private_computation_service_wrapper import (
    _build_private_computation_service,
)
from fbpcs.utils.config_yaml.config_yaml_dict import ConfigYamlDict


def parse_bolt_config(
    config: Dict[str, Any], logger: logging.Logger
) -> Tuple[BoltRunner, List[BoltJob]]:

    # create runner
    runner_config = config["runner"]
    runner = create_bolt_runner(runner_config=runner_config, logger=logger)

    # create jobs
    job_config_list = config["jobs"]
    bolt_job_list = create_job_list(job_config_list)
    return runner, bolt_job_list


def create_bolt_runner(
    runner_config: Dict[str, Any], logger: logging.Logger
) -> BoltRunner:
    publisher_client_config = ConfigYamlDict.from_file(
        runner_config["publisher_client_config"]
    )
    partner_client_config = ConfigYamlDict.from_file(
        runner_config["partner_client_config"]
    )
    publisher_client = BoltPCSClient(
        _build_private_computation_service(
            publisher_client_config["private_computation"],
            publisher_client_config["mpc"],
            publisher_client_config["pid"],
            publisher_client_config.get("post_processing_handlers", {}),
            publisher_client_config.get("pid_post_processing_handlers", {}),
        )
    )
    partner_client = BoltPCSClient(
        _build_private_computation_service(
            partner_client_config["private_computation"],
            partner_client_config["mpc"],
            partner_client_config["pid"],
            partner_client_config.get("post_processing_handlers", {}),
            partner_client_config.get("pid_post_processing_handlers", {}),
        )
    )

    runner = BoltRunner(
        publisher_client=publisher_client,
        partner_client=partner_client,
        max_parallel_runs=runner_config.get("max_parallel_runs"),
        logger=logger,
    )
    return runner


def create_job_list(job_config_list: Dict[str, Any]) -> List[BoltJob]:
    bolt_job_list = []
    for job_name, job_config in job_config_list.items():
        publisher_args = job_config["publisher"]
        publisher_args["role"] = "PUBLISHER"
        partner_args = job_config["partner"]
        partner_args["role"] = "PARTNER"
        shared_args = job_config["shared"]
        shared_args["job_name"] = job_name
        job_specific_args = job_config.get("job_args", {})

        publisher_create_instance_args = BoltPCSCreateInstanceArgs.from_yml_dict(
            {**publisher_args, **shared_args}
        )
        partner_create_instance_args = BoltPCSCreateInstanceArgs.from_yml_dict(
            {**partner_args, **shared_args}
        )
        publisher_bolt_args = BoltPlayerArgs(
            create_instance_args=publisher_create_instance_args,
            expected_result_path=publisher_args.get("expected_result_path"),
        )
        partner_bolt_args = BoltPlayerArgs(
            create_instance_args=partner_create_instance_args,
            expected_result_path=partner_args.get("expected_result_path"),
        )
        bolt_job = BoltJob(
            job_name=job_name,
            publisher_bolt_args=publisher_bolt_args,
            partner_bolt_args=partner_bolt_args,
            stage_flow=publisher_create_instance_args.stage_flow_cls,
            poll_interval=job_specific_args.get(
                "poll_interval", DEFAULT_POLL_INTERVAL_SEC
            ),
        )
        bolt_job_list.append(bolt_job)

    return bolt_job_list
