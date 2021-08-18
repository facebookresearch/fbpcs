#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
CLI for running a Private Attribute study


Usage:
    pa-coordinator create_instance <instance_id> --config=<config_file> --input_path=<input_path> --output_dir=<output_dir> --role=<pa_role> --num_pid_containers=<num_pid_containers> --num_mpc_containers=<num_mpc_containers> --num_files_per_mpc_container=<num_files_per_mpc_container> [--padding_size=<padding_size> --concurrency=<concurrency> --k_anonymity_threshold=<k_anonymity_threshold> --hmac_key=<base64_key>] [options]
    pa-coordinator id_match <instance_id> --config=<config_file> [--server_ips=<server_ips> --dry_run] [options]
    pa-coordinator prepare_compute_input <instance_id> --config=<config_file> [--dry_run --log_cost_to_s3] [options]
    pa-coordinator compute_attribution <instance_id> --config=<config_file> --game=<game_name> --attribution_rule=<attribution_rule> --aggregation_type=<aggregation_type> [--server_ips=<server_ips> --dry_run --log_cost_to_s3] [options]
    pa-coordinator aggregate_shards <instance_id> --config=<config_file> --game=<game_name> [--server_ips=<server_ips> --dry_run --log_cost_to_s3] [options]
    pa-coordinator get_server_ips  <instance_id> --config=<config_file> [options]
    pa-coordinator get_instance <instance_id> --config=<config_file> [options]
    pa-coordinator print_instance <instance_id> --config=<config_file> [options]


Options:
    -h --help                Show this help
    --log_path=<path>        Override the default path where logs are saved
    --verbose                Set logging level to DEBUG
"""
import logging
import os
from collections import defaultdict
from pathlib import Path, PurePath
from typing import Any, DefaultDict, Dict, List, Optional

import schema
from docopt import docopt
from fbpcp.entity.mpc_instance import MPCInstance
from fbpcp.service.container import ContainerService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.mpc import MPCService
from fbpcp.service.storage import StorageService
from fbpcp.util import reflect, yaml
from fbpmp.onedocker_binary_config import OneDockerBinaryConfig
from fbpmp.onedocker_service_config import OneDockerServiceConfig
from fbpmp.pid.entity.pid_instance import PIDInstance, PIDProtocol
from fbpmp.pid.service.pid_service.pid import PIDService
from fbpmp.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)
from fbpmp.private_computation.entity.private_computation_instance import (
    PrivateComputationRole,
)
from fbpmp.private_attribution.service.private_attribution import (
    PrivateAttributionService,
)

DEFAULT_HMAC_KEY: str = ""
DEFAULT_PADDING_SIZE: int = 4
DEFAULT_CONCURRENCY: int = 1
DEFAULT_K_ANONYMITY_THRESHOLD: int = 0


def _build_pa_service(
    pa_config: Dict[str, Any], mpc_config: Dict[str, Any], pid_config: Dict[str, Any]
) -> PrivateAttributionService:
    pa_instance_repository_config = pa_config["dependency"][
        "PrivateAttributionInstanceRepository"
    ]
    repository_class = reflect.get_class(pa_instance_repository_config["class"])
    repository_service = repository_class(
        **pa_instance_repository_config["constructor"]
    )
    onedocker_binary_config_map = _build_onedocker_binary_cfg_map(
        pa_config["dependency"]["OneDockerBinaryConfig"]
    )
    onedocker_service_config = _build_onedocker_service_cfg(
        pa_config["dependency"]["OneDockerServiceConfig"]
    )
    container_service = _build_container_service(
        pa_config["dependency"]["ContainerService"]
    )
    onedocker_service = _build_onedocker_service(
        container_service, onedocker_service_config.task_definition
    )
    storage_service = _build_storage_service(pa_config["dependency"]["StorageService"])
    return PrivateAttributionService(
        repository_service,
        _build_mpc_service(
            mpc_config,
            onedocker_service_config,
            container_service,
            storage_service
        ),
        _build_pid_service(
            pid_config,
            onedocker_service,
            storage_service,
            onedocker_binary_config_map,
        ),
        onedocker_service,
        onedocker_binary_config_map,
        storage_service,
    )


def _build_container_service(config: Dict[str, Any]) -> ContainerService:
    container_class = reflect.get_class(config["class"])
    return container_class(**config["constructor"])


def _build_onedocker_service(
    container_service: ContainerService,
    task_definition: str,
) -> OneDockerService:
    return OneDockerService(container_service, task_definition)


def _build_mpc_service(
    config: Dict[str, Any],
    onedocker_service_config: OneDockerServiceConfig,
    container_service: ContainerService,
    storage_service: StorageService,
) -> MPCService:

    mpcinstance_repository_config = config["dependency"]["MPCInstanceRepository"]
    repository_class = reflect.get_class(mpcinstance_repository_config["class"])
    repository_service = repository_class(
        **mpcinstance_repository_config["constructor"]
    )

    mpc_game_config = config["dependency"]["MPCGameService"]
    pa_game_repo_config = mpc_game_config["dependency"][
        "PrivateAttributionGameRepository"
    ]
    pa_game_repo_class = reflect.get_class(pa_game_repo_config["class"])
    pa_game_repo = pa_game_repo_class()
    mpc_game_class = reflect.get_class(mpc_game_config["class"])
    mpc_game_svc = mpc_game_class(pa_game_repo)

    task_definition = onedocker_service_config.task_definition

    return MPCService(
        container_service,
        storage_service,
        repository_service,
        task_definition,
        mpc_game_svc,
    )


def _build_onedocker_service_cfg(
    onedocker_service_config: Dict[str, Any]
) -> OneDockerServiceConfig:
    return OneDockerServiceConfig(**onedocker_service_config["constructor"])


def _build_onedocker_binary_cfg(
    onedocker_binary_config: Dict[str, Any]
) -> OneDockerBinaryConfig:
    return OneDockerBinaryConfig(**onedocker_binary_config["constructor"])


def _build_onedocker_binary_cfg_map(
    onedocker_binary_configs: Dict[str, Dict[str, Any]]
) -> DefaultDict[str, OneDockerBinaryConfig]:
    onedocker_binary_cfg_map = defaultdict(
        lambda: _build_onedocker_binary_cfg(onedocker_binary_configs["default"])
    )
    for binary_name, config in onedocker_binary_configs.items():
        onedocker_binary_cfg_map[binary_name] = _build_onedocker_binary_cfg(config)

    return onedocker_binary_cfg_map


def get_mpc(config: Dict[str, Any], instance_id: str, logger: logging.Logger) -> None:
    container_service = _build_container_service(
        config["private_attribution"]["dependency"]["ContainerService"]
    )
    storage_service = _build_storage_service(
        config["private_attribution"]["dependency"]["StorageService"]
    )
    mpc_service = _build_mpc_service(
        config["mpc"],
        _build_onedocker_service_cfg(config["private_attribution"]["dependency"]["OneDockerServiceConfig"]),
        container_service,
        storage_service
    )
    # calling update_instance here to get the newest container information
    instance = mpc_service.update_instance(instance_id)
    logger.info(instance)


def _build_pid_service(
    pid_config: Dict[str, Any],
    onedocker_service: OneDockerService,
    storage_service: StorageService,
    onedocker_binary_config_map: DefaultDict[str, OneDockerBinaryConfig],
) -> PIDService:
    pidinstance_repository_config = pid_config["dependency"]["PIDInstanceRepository"]
    repository_class = reflect.get_class(pidinstance_repository_config["class"])
    repository_service = repository_class(
        **pidinstance_repository_config["constructor"]
    )
    return PIDService(
        onedocker_service,
        storage_service,
        repository_service,
        onedocker_binary_config_map,
    )


def _build_storage_service(config: Dict[str, Any]) -> StorageService:
    storage_class = reflect.get_class(config["class"])
    return storage_class(**config["constructor"])


def create_instance(
    config: Dict[str, Any],
    instance_id: str,
    role: PrivateComputationRole,
    input_path: str,
    output_dir: str,
    hmac_key: str,
    num_pid_containers: int,
    num_mpc_containers: int,
    num_files_per_mpc_container: int,
    logger: logging.Logger,
    padding_size: int,
    concurrency: int = DEFAULT_CONCURRENCY,
    k_anonymity_threshold: int = DEFAULT_K_ANONYMITY_THRESHOLD,
) -> None:
    pa_service = _build_pa_service(
        config["private_attribution"], config["mpc"], config["pid"]
    )
    instance = pa_service.create_instance(
        instance_id=instance_id,
        role=role,
        input_path=input_path,
        output_dir=output_dir,
        hmac_key=hmac_key,
        num_pid_containers=num_pid_containers,
        num_mpc_containers=num_mpc_containers,
        num_files_per_mpc_container=num_files_per_mpc_container,
        padding_size=padding_size,
        concurrency=concurrency,
        k_anonymity_threshold=k_anonymity_threshold,
        logger=logger,
    )

    logger.info(instance)


def id_match(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger,
    server_ips: Optional[List[str]] = None,
    dry_run: Optional[bool] = False,
) -> None:
    pa_service = _build_pa_service(
        config["private_attribution"], config["mpc"], config["pid"]
    )

    # run pid instance through pid service invoked from pa service
    instance = pa_service.id_match(
        instance_id=instance_id,
        protocol=PIDProtocol.UNION_PID,
        pid_config=config["pid"],
        server_ips=server_ips,
        dry_run=dry_run,
    )

    logger.info(instance)


def prepare_compute_input(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger,
    dry_run: Optional[bool] = False,
    log_cost_to_s3: bool = False,
) -> None:
    pa_service = _build_pa_service(
        config["private_attribution"], config["mpc"], config["pid"]
    )

    uploaded_files = pa_service.prepare_data(
        instance_id=instance_id,
        dry_run=dry_run,
        log_cost_to_s3=log_cost_to_s3,
    )

    logging.info(f"Uploaded files: {uploaded_files}")
    logging.info("Finished preparing data")


def compute_attribution(
    config: Dict[str, Any],
    instance_id: str,
    game: str,
    attribution_rule: str,
    aggregation_type: str,
    logger: logging.Logger,
    server_ips: Optional[List[str]] = None,
    dry_run: Optional[bool] = False,
    log_cost_to_s3: bool = False,
) -> None:
    pa_service = _build_pa_service(
        config["private_attribution"], config["mpc"], config["pid"]
    )
    logging.info("Starting compute metrics...")

    instance = pa_service.compute_attribute(
        instance_id=instance_id,
        game_name=game,
        attribution_rule=attribution_rule,
        aggregation_type=aggregation_type,
        server_ips=server_ips,
        dry_run=dry_run,
        log_cost_to_s3=log_cost_to_s3,
    )

    logging.info("Finished running compute stage")
    logger.info(instance)


def aggregate_shards(
    config: Dict[str, Any],
    instance_id: str,
    game: str,
    logger: logging.Logger,
    server_ips: Optional[List[str]] = None,
    dry_run: Optional[bool] = False,
    log_cost_to_s3: bool = False,
) -> None:
    pa_service = _build_pa_service(
        config["private_attribution"], config["mpc"], config["pid"]
    )

    pa_service.update_instance(instance_id)

    instance = pa_service.aggregate_shards(
        instance_id=instance_id,
        game=game,
        server_ips=server_ips,
        dry_run=dry_run,
        log_cost_to_s3=log_cost_to_s3,
    )

    logger.info(instance)


def get_instance(
    config: Dict[str, Any], instance_id: str, logger: logging.Logger
) -> PrivateComputationInstance:
    pa_service = _build_pa_service(
        config["private_attribution"], config["mpc"], config["pid"]
    )

    pa_instance = pa_service.update_instance(instance_id)
    logger.info(pa_instance)
    return pa_instance


def get_server_ips(
    config: Dict[str, Any],
    instance_id: str,
) -> List[str]:
    pa_service = _build_pa_service(
        config["private_attribution"], config["mpc"], config["pid"]
    )

    pa_instance = pa_service.update_instance(instance_id)

    server_ips_list = None
    last_instance = pa_instance.instances[-1]
    if isinstance(last_instance, (PIDInstance, MPCInstance)):
        server_ips_list = last_instance.server_ips

    if not server_ips_list:
        server_ips_list = []
    print(*server_ips_list, sep=",")
    return server_ips_list

def print_instance(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger) -> None:
    print(get_instance(config, instance_id, logger))


def main() -> None:
    s = schema.Schema(
        {
            "create_instance": bool,
            "get_instance": bool,
            "print_instance": bool,
            "id_match": bool,
            "prepare_compute_input": bool,
            "compute_attribution": bool,
            "aggregate_shards": bool,
            "get_server_ips": bool,
            "<instance_id>": schema.Or(None, str),
            "--config": schema.And(schema.Use(PurePath), os.path.exists),
            "--input_path": schema.Or(None, str),
            "--output_dir": schema.Or(None, str),
            "--game": schema.Or(None, str),
            "--aggregation_type": schema.Or(None, str),
            "--attribution_rule": schema.Or(None, str),
            "--num_pid_containers": schema.Or(None, schema.Use(int)),
            "--num_mpc_containers": schema.Or(None, schema.Use(int)),
            "--num_files_per_mpc_container": schema.Or(None, schema.Use(int)),
            "--padding_size": schema.Or(None, schema.Use(int)),
            "--role": schema.Or(
                None,
                schema.And(
                    schema.Use(str.upper),
                    lambda s: s in ("PUBLISHER", "PARTNER"),
                    schema.Use(PrivateComputationRole),
                ),
            ),
            "--k_anonymity_threshold": schema.Or(None, schema.Use(int)),
            "--server_ips": schema.Or(None, schema.Use(lambda arg: arg.split(","))),
            "--concurrency": schema.Or(None, schema.Use(int)),
            "--hmac_key": schema.Or(None, str),
            "--dry_run": bool,
            "--log_path": schema.Or(None, schema.Use(Path)),
            "--log_cost_to_s3": schema.Or(None, schema.Use(bool)),
            "--verbose": bool,
            "--help": bool,
        }
    )

    arguments = s.validate(docopt(__doc__))
    config = yaml.load(Path(arguments["--config"]))

    log_path = arguments["--log_path"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO
    logging.basicConfig(
        filename=log_path,
        level=log_level,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    )
    logger = logging.getLogger(__name__)

    instance_id = arguments["<instance_id>"]

    if arguments["create_instance"]:
        logger.info(f"Create instance: {instance_id}")

        # Optional arguments
        hmac_key: Optional[str] = arguments["--hmac_key"]
        padding_size: Optional[int] = arguments["--padding_size"]
        concurrency: Optional[int] = arguments["--concurrency"]
        k_anonymity_threshold: Optional[int] = arguments["--k_anonymity_threshold"]

        create_instance(
            instance_id=instance_id,
            config=config,
            input_path=arguments["--input_path"],
            output_dir=arguments["--output_dir"],
            role=arguments["--role"],
            hmac_key=hmac_key or DEFAULT_HMAC_KEY,
            num_pid_containers=arguments["--num_pid_containers"],
            num_mpc_containers=arguments["--num_mpc_containers"],
            num_files_per_mpc_container=arguments["--num_files_per_mpc_container"],
            padding_size=padding_size or DEFAULT_PADDING_SIZE,
            concurrency=concurrency or DEFAULT_CONCURRENCY,
            k_anonymity_threshold=k_anonymity_threshold
            or DEFAULT_K_ANONYMITY_THRESHOLD,
            logger=logger,
        )
    elif arguments["get_instance"]:
        logger.info(f"Get instance: {instance_id}")
        get_instance(
            config=config,
            instance_id=instance_id,
            logger=logger,
        )
    elif arguments["id_match"]:
        logger.info(f"Run id match on instance: {instance_id}")
        id_match(
            config=config,
            instance_id=instance_id,
            logger=logger,
            server_ips=arguments["--server_ips"],
            dry_run=arguments["--dry_run"],
        )
    elif arguments["prepare_compute_input"]:
        logger.info(f"Run id match on instance: {instance_id}")
        prepare_compute_input(
            config=config,
            instance_id=instance_id,
            logger=logger,
            dry_run=arguments["--dry_run"],
            log_cost_to_s3=arguments["--log_cost_to_s3"],
        )
    elif arguments["compute_attribution"]:
        logger.info(f"Compute instance: {instance_id}")
        compute_attribution(
            config=config,
            instance_id=instance_id,
            game=arguments["--game"],
            attribution_rule=arguments["--attribution_rule"],
            aggregation_type=arguments["--aggregation_type"],
            server_ips=arguments["--server_ips"],
            logger=logger,
            dry_run=arguments["--dry_run"],
            log_cost_to_s3=arguments["--log_cost_to_s3"],
        )
    elif arguments["aggregate_shards"]:
        aggregate_shards(
            config=config,
            instance_id=instance_id,
            game=arguments["--game"],
            server_ips=arguments["--server_ips"],
            logger=logger,
            dry_run=arguments["--dry_run"],
            log_cost_to_s3=arguments["--log_cost_to_s3"],
        )
    elif arguments["get_server_ips"]:
        get_server_ips(
            config=config,
            instance_id=instance_id,
        )
    elif arguments["print_instance"]:
        print_instance(
            config=config,
            instance_id=instance_id,
            logger=logger,
        )


if __name__ == "__main__":
    main()
