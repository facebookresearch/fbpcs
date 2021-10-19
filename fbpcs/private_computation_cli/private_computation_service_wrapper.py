#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import logging
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional

from fbpcp.entity.mpc_instance import MPCInstance
from fbpcp.service.container import ContainerService
from fbpcp.service.mpc import MPCService
from fbpcp.service.onedocker import OneDockerService
from fbpcp.service.storage import StorageService
from fbpcp.util import reflect
from fbpcs.data_processing.sharding.sharding import ShardingService
from fbpcs.onedocker_binary_config import OneDockerBinaryConfig
from fbpcs.onedocker_service_config import OneDockerServiceConfig
from fbpcs.pid.entity.pid_instance import PIDInstance
from fbpcs.pid.service.pid_service.pid import PIDService
from fbpcs.post_processing_handler.post_processing_handler import PostProcessingHandler
from fbpcs.private_computation.entity.private_computation_instance import (
    AggregationType,
    AttributionRule,
    PrivateComputationGameType,
    PrivateComputationRole,
    PrivateComputationInstance,
)
from fbpcs.private_computation.service.constants import STAGE_STARTED_STATUSES
from fbpcs.private_computation.service.private_computation import (
    PrivateComputationService,
)

DEFAULT_CONCURRENCY = 4


def create_instance(
    config: Dict[str, Any],
    instance_id: str,
    role: PrivateComputationRole,
    game_type: PrivateComputationGameType,
    logger: logging.Logger,
    input_path: str,
    output_dir: str,
    num_pid_containers: int,
    num_mpc_containers: int,
    attribution_rule: Optional[AttributionRule] = None,
    aggregation_type: Optional[AggregationType] = None,
    concurrency: Optional[int] = None,
    hmac_key: Optional[str] = None,
    num_files_per_mpc_container: Optional[int] = None,
    padding_size: Optional[int] = None,
    k_anonymity_threshold: Optional[int] = None,
    fail_fast: bool = False,
) -> PrivateComputationInstance:
    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )
    instance = pc_service.create_instance(
        instance_id=instance_id,
        role=role,
        game_type=game_type,
        input_path=input_path,
        output_dir=output_dir,
        num_pid_containers=num_pid_containers,
        num_mpc_containers=num_mpc_containers,
        concurrency=concurrency or DEFAULT_CONCURRENCY,
        attribution_rule=attribution_rule,
        aggregation_type=aggregation_type,
        num_files_per_mpc_container=num_files_per_mpc_container,
        is_validating=config["private_computation"]["dependency"]["ValidationConfig"][
            "is_validating"
        ],
        hmac_key=hmac_key,
        padding_size=padding_size,
        k_anonymity_threshold=k_anonymity_threshold,
        fail_fast=fail_fast,
    )

    logger.info(instance)
    return instance


def id_match(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger,
    server_ips: Optional[List[str]] = None,
    dry_run: Optional[bool] = False,
) -> None:
    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )

    # run pid instance through pid service invoked from pc service
    instance = pc_service.id_match(
        instance_id=instance_id,
        is_validating=config["private_computation"]["dependency"]["ValidationConfig"][
            "is_validating"
        ],
        synthetic_shard_path=config["private_computation"]["dependency"][
            "ValidationConfig"
        ]["synthetic_shard_path"],
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
    private_computation_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )

    # Because it's possible that the "get" command never gets called to update the instance since the last step started,
    # so it could appear that the current status is still XXX_STARTED when it should be XXX_FAILED or XXX_COMPLETED,
    # so we need to explicitly call update_instance() here to get the current status.
    private_computation_service.update_instance(instance_id)

    logging.info("Starting preparing data")

    private_computation_service.prepare_data(
        instance_id=instance_id,
        is_validating=config["private_computation"]["dependency"]["ValidationConfig"][
            "is_validating"
        ],
        dry_run=dry_run,
        log_cost_to_s3=log_cost_to_s3,
    )

    logging.info("Finished preparing data")


def compute_metrics(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger,
    server_ips: Optional[List[str]] = None,
    dry_run: Optional[bool] = False,
    log_cost_to_s3: bool = False,
) -> None:
    private_computation_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )

    logging.info("Starting compute metrics...")

    instance = private_computation_service.compute_metrics(
        instance_id=instance_id,
        is_validating=config["private_computation"]["dependency"]["ValidationConfig"][
            "is_validating"
        ],
        server_ips=server_ips,
        dry_run=dry_run,
        log_cost_to_s3=log_cost_to_s3,
    )

    logging.info("Finished running compute stage")
    logger.info(instance)


def aggregate_shards(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger,
    server_ips: Optional[List[str]] = None,
    dry_run: Optional[bool] = False,
    log_cost_to_s3: bool = False,
) -> None:
    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )

    # This call is necessary because it could be the case that last aggregate failed and this is a valid retry,
    # or last compute succeeded and this is a regular aggregate. Either way, because it's possible that
    # the "get" command never gets called to update the instance since the last step started, so it appears that the
    # current status is still COMPUTATION_STARTED or AGGREGATION_STARTED, which is an invalid status for retry.
    pc_service.update_instance(instance_id)

    instance = pc_service.aggregate_shards(
        instance_id=instance_id,
        is_validating=config["private_computation"]["dependency"]["ValidationConfig"][
            "is_validating"
        ],
        server_ips=server_ips,
        dry_run=dry_run,
        log_cost_to_s3=log_cost_to_s3,
    )

    logger.info(instance)


def validate(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger,
    aggregated_result_path: str,
    expected_result_path: str,
) -> None:
    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )
    pc_service.validate_metrics(
        instance_id=instance_id,
        aggregated_result_path=aggregated_result_path,
        expected_result_path=expected_result_path,
    )


def _get_post_processing_handlers(
    config: Dict[str, Any]
) -> Dict[str, PostProcessingHandler]:
    if not config:
        return {}
    return {
        name: reflect.get_class(handler_config["class"])(
            **handler_config.get("constructor", {})
        )
        for name, handler_config in config["dependency"].items()
    }


def run_post_processing_handlers(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger,
    aggregated_result_path: Optional[str] = None,
    dry_run: Optional[bool] = False,
) -> None:

    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )

    instance = pc_service.run_post_processing_handlers(
        instance_id=instance_id,
        aggregated_result_path=aggregated_result_path,
        dry_run=dry_run,
    )

    logger.info(instance)

def run_next(
    config: Dict[str, Any],
    instance_id: str,
    logger: logging.Logger,
    server_ips: Optional[List[str]] = None,
) -> None:

    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )

    # Because it's possible that the "get" command never gets called to update the instance since the last step started,
    # so it could appear that the current status is still XXX_STARTED when it should be XXX_FAILED or XXX_COMPLETED,
    # so we need to explicitly call update_instance() here to get the current status.
    pc_service.update_instance(instance_id)

    instance = pc_service.run_next(
        instance_id=instance_id,
        server_ips=server_ips
    )

    logger.info(instance)


def get_instance(
    config: Dict[str, Any], instance_id: str, logger: logging.Logger
) -> PrivateComputationInstance:
    """
    To get the updated status of the pc instance with id instance_id.
    We only call pc_service.update_instance() under XXX_STARTED status because otherwise we could run into
    a race condition: when status is not XXX_STARTED, PrivateComputationService might be writing a new PID or
    MPCInstance to this pc instance. Because pc_service.update_instance() also writes to this pc instance, it could
    accidentally erase that PID or MPCInstance.
    """
    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )
    instance = pc_service.get_instance(instance_id)
    if instance.status in STAGE_STARTED_STATUSES:
        instance = pc_service.update_instance(instance_id)
    logger.info(instance)
    return instance


def get_server_ips(
    config: Dict[str, Any], instance_id: str, logger: logging.Logger
) -> List[str]:
    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )

    pc_instance = pc_service.instance_repository.read(instance_id)

    # This utility should only be used to get ips from a publisher instance
    if pc_instance.role is not PrivateComputationRole.PUBLISHER:
        logger.warning("Unable to get server ips from a partner instance")
        return []

    server_ips_list = None
    last_instance = pc_instance.instances[-1]
    if isinstance(last_instance, (PIDInstance, MPCInstance)):
        server_ips_list = last_instance.server_ips

    if server_ips_list is None:
        server_ips_list = []

    print(*server_ips_list, sep=",")
    return server_ips_list


def get_pid(config: Dict[str, Any], instance_id: str, logger: logging.Logger) -> None:
    container_service = _build_container_service(
        config["private_computation"]["dependency"]["ContainerService"]
    )
    onedocker_service_config = _build_onedocker_service_cfg(
        config["private_computation"]["dependency"]["OneDockerServiceConfig"]
    )
    onedocker_binary_config_map = _build_onedocker_binary_cfg_map(
        config["private_computation"]["dependency"]["OneDockerBinaryConfig"]
    )
    onedocker_service = _build_onedocker_service(
        container_service, onedocker_service_config.task_definition
    )
    storage_service = _build_storage_service(
        config["private_computation"]["dependency"]["StorageService"]
    )
    pid_service = _build_pid_service(
        config["pid"],
        onedocker_service,
        storage_service,
        onedocker_binary_config_map,
    )
    instance = pid_service.get_instance(instance_id)
    logger.info(instance)


def get_mpc(config: Dict[str, Any], instance_id: str, logger: logging.Logger) -> None:
    container_service = _build_container_service(
        config["private_computation"]["dependency"]["ContainerService"]
    )
    storage_service = _build_storage_service(
        config["private_computation"]["dependency"]["StorageService"]
    )
    mpc_service = _build_mpc_service(
        config["mpc"],
        _build_onedocker_service_cfg(
            config["private_computation"]["dependency"]["OneDockerServiceConfig"]
        ),
        container_service,
        storage_service,
    )
    # calling update_instance here to get the newest container information
    instance = mpc_service.update_instance(instance_id)
    logger.info(instance)


def cancel_current_stage(
    config: Dict[str, Any], instance_id: str, logger: logging.Logger
) -> PrivateComputationInstance:
    pc_service = _build_private_computation_service(
        config["private_computation"],
        config["mpc"],
        config["pid"],
        config.get("post_processing_handlers", {}),
    )
    instance = pc_service.cancel_current_stage(instance_id=instance_id)
    logger.info("Done canceling the current stage")
    return instance


def print_instance(
    config: Dict[str, Any], instance_id: str, logger: logging.Logger
) -> None:
    print(get_instance(config, instance_id, logger))


def _build_container_service(config: Dict[str, Any]) -> ContainerService:
    container_class = reflect.get_class(config["class"])
    return container_class(**config["constructor"])


def _build_storage_service(config: Dict[str, Any]) -> StorageService:
    storage_class = reflect.get_class(config["class"])
    return storage_class(**config["constructor"])


def _build_sharding_service(config: Dict[str, Any]) -> ShardingService:
    sharding_class = reflect.get_class(config["class"])
    return sharding_class()


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
    pc_game_repo_config = mpc_game_config["dependency"][
        "PrivateComputationGameRepository"
    ]
    pc_game_repo_class = reflect.get_class(pc_game_repo_config["class"])
    pc_game_repo = pc_game_repo_class()
    mpc_game_class = reflect.get_class(mpc_game_config["class"])
    mpc_game_svc = mpc_game_class(pc_game_repo)

    task_definition = onedocker_service_config.task_definition

    return MPCService(
        container_service,
        repository_service,
        task_definition,
        mpc_game_svc,
    )


def _build_private_computation_service(
    pc_config: Dict[str, Any],
    mpc_config: Dict[str, Any],
    pid_config: Dict[str, Any],
    pph_config: Dict[str, Any],
) -> PrivateComputationService:
    instance_repository_config = pc_config["dependency"][
        "PrivateComputationInstanceRepository"
    ]
    repository_class = reflect.get_class(instance_repository_config["class"])
    repository_service = repository_class(**instance_repository_config["constructor"])
    container_service = _build_container_service(
        pc_config["dependency"]["ContainerService"]
    )
    onedocker_service_config = _build_onedocker_service_cfg(
        pc_config["dependency"]["OneDockerServiceConfig"]
    )
    onedocker_binary_config_map = _build_onedocker_binary_cfg_map(
        pc_config["dependency"]["OneDockerBinaryConfig"]
    )
    onedocker_service = _build_onedocker_service(
        container_service, onedocker_service_config.task_definition
    )
    storage_service = _build_storage_service(pc_config["dependency"]["StorageService"])
    return PrivateComputationService(
        repository_service,
        storage_service,
        _build_mpc_service(
            mpc_config, onedocker_service_config, container_service, storage_service
        ),
        _build_pid_service(
            pid_config,
            onedocker_service,
            storage_service,
            onedocker_binary_config_map,
        ),
        onedocker_service,
        onedocker_binary_config_map,
        pid_config,
        _get_post_processing_handlers(pph_config),
    )


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
