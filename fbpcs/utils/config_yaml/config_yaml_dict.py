#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from pathlib import Path
from typing import Any, Dict

from fbpcp.util import yaml
from fbpcs.utils.config_yaml.exceptions import (
    ConfigYamlFieldNotFoundError,
    ConfigYamlFileParsingError,
    ConfigYamlValidationError,
)
from yaml import YAMLError


class ConfigYamlDict(Dict[str, Any]):
    """Wrapper around dict that throws a custom KeyError exception to inform the user
    that there is something wrong with their config.yml file."""

    def __getitem__(self, key: str) -> Any:
        """Override of dict key access, e.g. x = my_dict[key]"""
        try:
            val = super().__getitem__(key)
        except KeyError:
            raise ConfigYamlFieldNotFoundError(key) from None

        if val == "TODO":
            raise ConfigYamlValidationError(
                key,
                "TODOs found in config",
                "Fill in remaining TODO entries in config.yml",
            )
        return val

    def __setitem__(self, key: str, value: Any) -> None:
        """Override of dict item setting, e.g. my_dict[key] = x.

        Specifically, if value is a dict, it converts the dict to ConfigYamlDict
        """
        value = self.from_dict(value) if isinstance(value, dict) else value
        super().__setitem__(key, value)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ConfigYamlDict":
        """Converts a normal dictionary to a ConfigYamlDict"""
        my_dict = cls()
        for k, v in d.items():
            my_dict[k] = v
        return my_dict

    @classmethod
    def from_file(cls, config_file_path: str) -> "ConfigYamlDict":
        """Read a yaml file to a ConfigYamlDict"""
        try:
            config_dict = yaml.load(Path(config_file_path))
        except YAMLError as e:
            raise ConfigYamlFileParsingError(config_file_path, str(e))
        return ConfigYamlDict.from_dict(config_dict)
