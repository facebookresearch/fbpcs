#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


"""
CLI for running a Private Lift study


Usage:
    pl-coordinator create_instance <instance_id> --config=<config_file> --role=<pl_role> [--num_containers=<num_containers> --input_path=<input_path> --output_dir=<output_dir>] [options]
    pl-coordinator id_match <instance_id> --config=<config_file> [--num_containers=<num_containers> --input_path=<input_path> --output_path=<output_path> --server_ips=<server_ips> --hmac_key=<base64_key> --fail_fast --dry_run] [options]
    pl-coordinator compute <instance_id> --config=<config_file> [(--num_containers=<num_containers> --spine_path=<spine_path> --data_path=<data_path>) --output_path=<output_path> --server_ips=<server_ips> --concurrency=<concurrency> --dry_run] [options]
    pl-coordinator aggregate <instance_id> --config=<config_file> [(--input_path=<input_path> --num_shards=<num_shards>) --output_path=<output_path> --server_ips=<server_ips> --dry_run] [options]
    pl-coordinator validate <instance_id> --config=<config_file> --aggregated_result_path=<aggregated_result_path> --expected_result_path=<expected_result_path> [options]
    pl-coordinator run_post_processing_handlers <instance_id> --config=<config_file> [--aggregated_result_path=<aggregated_result_path> --dry_run] [options]
    pl-coordinator get <instance_id> --config=<config_file> [options]
    pl-coordinator get_server_ips <instance_id> --config=<config_file> [options]
    pl-coordinator get_pid <instance_id> --config=<config_file> [options]
    pl-coordinator get_mpc <instance_id> --config=<config_file> [options]
    pl-coordinator run_instance <instance_id> --config=<config_file> --input_path=<input_path> [--tries_per_stage=<tries_per_stage> --dry_run] [options]
    pl-coordinator run_instances <instance_ids> --config=<config_file> --input_paths=<input_paths> [--tries_per_stage=<tries_per_stage> --dry_run] [options]
    pl-coordinator run_study <study_id> --config=<config_file> --objective_ids=<objective_ids> --input_paths=<input_paths> [--tries_per_stage=<tries_per_stage> --dry_run] [options]
    pl-coordinator cancel_current_stage <instance_id> --config=<config_file> [options]

Options:
    -h --help                Show this help
    --log_path=<path>        Override the default path where logs are saved
    --verbose                Set logging level to DEBUG
"""

import logging
import os
from pathlib import Path, PurePath

import schema
from docopt import docopt
from fbpcp.util import yaml
from fbpmp.pl_coordinator.pl_instance_runner import run_instance, run_instances
from fbpmp.pl_coordinator.pl_service_wrapper import (
    aggregate,
    compute,
    create_instance,
    get,
    get_mpc,
    get_pid,
    get_server_ips,
    id_match,
    run_post_processing_handlers,
    validate,
    cancel_current_stage,
)
from fbpmp.pl_coordinator.pl_study_runner import run_study
from fbpmp.private_computation.entity.private_computation_instance import PrivateComputationRole


def main():
    s = schema.Schema(
        {
            "create_instance": bool,
            "id_match": bool,
            "compute": bool,
            "aggregate": bool,
            "validate": bool,
            "run_post_processing_handlers": bool,
            "get": bool,
            "get_server_ips": bool,
            "get_pid": bool,
            "get_mpc": bool,
            "run_instance": bool,
            "run_instances": bool,
            "run_study": bool,
            "cancel_current_stage": bool,
            "<instance_id>": schema.Or(None, str),
            "<instance_ids>": schema.Or(None, schema.Use(lambda arg: arg.split(","))),
            "<study_id>": schema.Or(None, str),
            "--config": schema.And(schema.Use(PurePath), os.path.exists),
            "--role": schema.Or(
                None,
                schema.And(
                    schema.Use(str.upper),
                    lambda s: s in ("PUBLISHER", "PARTNER"),
                    schema.Use(PrivateComputationRole),
                ),
            ),
            "--objective_ids": schema.Or(None, schema.Use(lambda arg: arg.split(","))),
            "--input_path": schema.Or(None, str),
            "--input_paths": schema.Or(None, schema.Use(lambda arg: arg.split(","))),
            "--spine_path": schema.Or(None, str),
            "--data_path": schema.Or(None, str),
            "--output_path": schema.Or(None, str),
            "--output_dir": schema.Or(None, str),
            "--aggregated_result_path": schema.Or(None, str),
            "--expected_result_path": schema.Or(None, str),
            "--num_containers": schema.Or(None, schema.Use(int)),
            "--num_shards": schema.Or(None, schema.Use(int)),
            "--server_ips": schema.Or(None, schema.Use(lambda arg: arg.split(","))),
            "--concurrency": schema.Or(None, schema.Use(int)),
            "--hmac_key": schema.Or(None, str),
            "--tries_per_stage": schema.Or(None, schema.Use(int)),
            "--fail_fast": bool,
            "--dry_run": bool,
            "--log_path": schema.Or(None, schema.Use(Path)),
            "--verbose": bool,
            "--help": bool,
        }
    )

    arguments = s.validate(docopt(__doc__))
    config = yaml.load(Path(arguments["--config"]))

    log_path = arguments["--log_path"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO
    instance_id = arguments["<instance_id>"]

    logging.basicConfig(filename=log_path, level=log_level)
    logger = logging.getLogger(__name__)

    if arguments["create_instance"]:
        logger.info(f"Create instance: {instance_id}")
        create_instance(
            config=config,
            instance_id=instance_id,
            role=arguments["--role"],
            logger=logger,
            num_containers=arguments["--num_containers"],
            input_path=arguments["--input_path"],
            output_dir=arguments["--output_dir"],
        )
    elif arguments["id_match"]:
        logger.info(f"Run id match on instance: {instance_id}")
        id_match(
            config=config,
            instance_id=instance_id,
            num_containers=arguments["--num_containers"],
            input_path=arguments["--input_path"],
            output_path=arguments["--output_path"],
            logger=logger,
            fail_fast=arguments["--fail_fast"],
            server_ips=arguments["--server_ips"],
            hmac_key=arguments["--hmac_key"],
            dry_run=arguments["--dry_run"],
        )
    elif arguments["compute"]:
        if (
            arguments["--spine_path"] == arguments["--output_path"]
            or arguments["--data_path"] == arguments["--output_path"]
        ):
            raise ValueError(
                "spine_path/data_path and output_path must NOT be the same."
            )
        logger.info(f"Compute instance: {instance_id}")
        compute(
            config=config,
            instance_id=instance_id,
            num_containers=arguments["--num_containers"],
            spine_path=arguments["--spine_path"],
            data_path=arguments["--data_path"],
            output_path=arguments["--output_path"],
            concurrency=arguments["--concurrency"],
            logger=logger,
            server_ips=arguments["--server_ips"],
            dry_run=arguments["--dry_run"],
        )
    elif arguments["run_post_processing_handlers"]:
        logger.info(f"post processing handlers instance: {instance_id}")
        run_post_processing_handlers(
            config=config,
            instance_id=instance_id,
            logger=logger,
            aggregated_result_path=arguments["--aggregated_result_path"],
            dry_run=arguments["--dry_run"],
        )
    elif arguments["get"]:
        logger.info(f"Get instance: {instance_id}")
        get(config, instance_id, logger)
    elif arguments["get_server_ips"]:
        get_server_ips(config, instance_id, logger)
    elif arguments["get_pid"]:
        logger.info(f"Get PID instance: {instance_id}")
        get_pid(config, instance_id, logger)
    elif arguments["get_mpc"]:
        logger.info(f"Get MPC instance: {instance_id}")
        get_mpc(config, instance_id, logger)
    elif arguments["aggregate"]:
        logger.info(f"Aggregate instance: {instance_id}")
        aggregate(
            config=config,
            instance_id=instance_id,
            output_path=arguments["--output_path"],
            logger=logger,
            input_path=arguments["--input_path"],
            num_shards=arguments["--num_shards"],
            server_ips=arguments["--server_ips"],
            dry_run=arguments["--dry_run"],
        )
    elif arguments["validate"]:
        logger.info(f"Vallidate instance: {instance_id}")
        validate(
            config=config,
            instance_id=instance_id,
            aggregated_result_path=arguments["--aggregated_result_path"],
            expected_result_path=arguments["--expected_result_path"],
            logger=logger,
        )
    elif arguments["run_instance"]:
        logger.info(f"Running instance: {instance_id}")
        run_instance(
            config=config,
            instance_id=instance_id,
            input_path=arguments["--input_path"],
            logger=logger,
            num_tries=arguments["--tries_per_stage"],
            dry_run=arguments["--dry_run"],
        )
    elif arguments["run_instances"]:
        run_instances(
            config=config,
            instance_ids=arguments["<instance_ids>"],
            input_paths=arguments["--input_paths"],
            logger=logger,
            num_tries=arguments["--tries_per_stage"],
            dry_run=arguments["--dry_run"],
        )
    elif arguments["run_study"]:
        run_study(
            config=config,
            study_id=arguments["<study_id>"],
            objective_ids=arguments["--objective_ids"],
            input_paths=arguments["--input_paths"],
            logger=logger,
            num_tries=arguments["--tries_per_stage"],
            dry_run=arguments["--dry_run"],
        )
    elif arguments["cancel_current_stage"]:
        logger.info(f"Canceling the current running stage of instance: {instance_id}")
        cancel_current_stage(
            config=config,
            instance_id=instance_id,
            logger=logger,
        )


if __name__ == "__main__":
    main()
