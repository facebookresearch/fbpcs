#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Dict, Type, TypeVar

from fbpcp.util.reflect import get_class as fbpcp_get_class
from fbpcs.utils.config_yaml.exceptions import (
    ConfigYamlModuleImportError,
    ConfigYamlClassNotFoundError,
    ConfigYamlWrongClassConfiguredError,
    ConfigYamlWrongConstructorError,
)

T = TypeVar("T")

def get_class(class_path: str, target_class: Type[T]) -> Type[T]:
    """Convert a python module class path to a class object. Class path expected to be extracted from private computation config.yml file.

    Arguments:
        class_path: module path to class, e.g. fbpcp.service.container_aws.AWSContainerService
        target_class: the type of class expected at class_path, e.g. ContainerService

    Raises:
        ConfigYamlModuleImportError: the module path could not be imported
        ConfigYamlClassNotFoundError: the class could not be found in the module
        ConfigYamlWrongClassConfiguredError: a class was found, but it is different from the target_class

    Returns:
        A class object of type target_class
    """
    try:
        cls = fbpcp_get_class(class_path)
        assert issubclass(cls, target_class)
    except ImportError:
        raise ConfigYamlModuleImportError(class_path) from None
    except AttributeError:
        raise ConfigYamlClassNotFoundError(class_path) from None
    except AssertionError:
        raise ConfigYamlWrongClassConfiguredError(
            class_path, target_class.__name__
        ) from None
    return cls

def get_instance(config: Dict[str, Any], target_class: Type[T]) -> T:
    """Constructs an instance of type target_class from config.yml dict structure.

    E.g.

    {class: path.to.module.MyClass, constructor: {arg1: <arg1>}}

    returns MyClass(arg1)

    Raises:
        ConfigYamlWrongConstructorError: incorrect arguments passed to class constructor

    Returns:
        instance of type target_class


    """
    cls = get_class(config["class"], target_class)
    try:
        instance = cls(**config.get("constructor", {}))
    except TypeError as e:
        raise ConfigYamlWrongConstructorError(cls.__name__, str(e)) from None
    return instance
