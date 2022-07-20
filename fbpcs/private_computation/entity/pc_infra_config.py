# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from fbpcs.private_computation.entity.exceptions import CannotFindDependencyError

from fbpcs.private_computation.entity.pc_infra_config_data import (
    PrivateComputationInfraConfigInfo,
)


@dataclass
class PrivateComputationInfraConfig:
    cloud: str
    base_dir: Optional[str]
    region: Optional[str]
    cluster: Optional[str]
    subnets: Optional[str]
    tmp_directory: str
    binary_version: Optional[str]
    task_definition: Optional[str]

    def __init__(self, infra_config: Dict[str, Any]) -> None:
        self.cloud = infra_config.get("cloud", "AWS")
        self.base_dir = infra_config.get("base_dir", None)
        self.region = infra_config.get("region", None)
        self.cluster = infra_config.get("cluster", None)
        self.subnets = infra_config.get("subnets", None)
        self.tmp_directory = infra_config.get("tmp_directory", "/tmp")
        self.binary_version = infra_config.get("binary_version", "latest")
        self.task_definition = infra_config.get("task_definition", None)

    @classmethod
    def build_full_config(cls, yml_config: Dict[str, Any]) -> Dict[str, Any]:
        input_config = yml_config["private_computation"]

        if "infra_config" in input_config:
            # if this is new version -> convert into old version
            yml_config = input_config

            infra_config = cls(yml_config.pop("infra_config"))

            pc = infra_config._generate_pc()

            pid = infra_config._generate_pid()
            mpc = infra_config._generate_mpc()

            yml_config["private_computation"] = pc
            yml_config["pid"] = pid
            yml_config["mpc"] = mpc

            # Partner can only override dependencies
            if "overrides" in yml_config:
                overrides = yml_config.pop("overrides")
                # can handle more than 1 override
                for dep_key, dep_value in overrides.items():
                    if dep_key in yml_config["private_computation"]["dependency"]:
                        yml_config["private_computation"]["dependency"][
                            dep_key
                        ] = dep_value

                    elif dep_key in yml_config["mpc"]["dependency"]:
                        yml_config["mpc"]["dependency"][dep_key] = dep_value

                    else:
                        raise CannotFindDependencyError(dep_key)

        return yml_config

    #############################
    # Helper functions
    #############################

    @classmethod
    def _get_defaults(cls) -> Dict[str, PrivateComputationInfraConfigInfo]:
        """
        Return info for the classes with name and constructor arguments in PC-dependency parts in config.yml.

        """
        dict_defaults = {
            "PrivateComputationInstanceRepository": PrivateComputationInfraConfigInfo.PC_INSTANCE_REPO,
            "ContainerService": PrivateComputationInfraConfigInfo.CONTAINER_SERVICE,
            "StorageService": PrivateComputationInfraConfigInfo.STORAGE_SERVICE,
            "PCValidatorConfig": PrivateComputationInfraConfigInfo.PC_VALIDATOR_CONFIG,
        }
        return dict_defaults

    def _generate_pc(self) -> Dict[str, Any]:
        pc = {}
        pc["dependency"] = {}

        # add ValidationConfig
        pc["dependency"]["ValidationConfig"] = {
            "is_validating": False,
            "synthetic_shard_path": None,
        }
        # add OneDockerBinaryConfig
        pc["dependency"]["OneDockerBinaryConfig"] = {
            "default": {
                "constructor": {
                    "tmp_directory": self.tmp_directory,
                    "binary_version": self.binary_version,
                }
            }
        }
        # add OneDockerServiceConfig
        pc["dependency"]["OneDockerServiceConfig"] = {
            "constructor": {"task_definition": self.task_definition}
        }

        # add other dependencies
        dependencies = self._get_defaults()
        for dep_key, dep_value in dependencies.items():
            self._generate_dependency(pc, dep_key, dep_value)

        return pc

    def _generate_pid(self) -> Dict[str, Any]:
        pid = {}
        pid["dependency"] = None
        return pid

    def _generate_mpc(self) -> Dict[str, Any]:
        mpc = {}
        mpc["dependency"] = {}

        class_name_service = (
            PrivateComputationInfraConfigInfo.MPC_GAME_SERVICE.value.cls_name
        )
        class_name_game_repo = (
            PrivateComputationInfraConfigInfo.PC_GAME_REPO.value.cls_name
        )

        mpc["dependency"]["MPCGameService"] = {
            "class": class_name_service,
            "dependency": {
                "PrivateComputationGameRepository": {
                    "class": class_name_game_repo,
                }
            },
        }

        self._generate_dependency(
            mpc,
            "MPCInstanceRepository",
            PrivateComputationInfraConfigInfo.MPC_INSTANCE_REPO,
        )

        return mpc

    def _generate_dependency(
        self,
        dep_dict: Dict[str, Any],
        dep_name: str,
        dep_value: PrivateComputationInfraConfigInfo,
    ) -> None:
        constructor = self._generate_constructor(dep_value.value.args)
        dep_dict["dependency"][dep_name] = {
            "class": dep_value.value.cls_name,
            "constructor": constructor,
        }

    def _generate_constructor(self, args: Set[str]) -> Dict[str, Any]:
        constructor = {}
        for arg in args:
            constructor[arg] = getattr(self, arg)
        return constructor
