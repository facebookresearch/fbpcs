#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from abc import ABC, abstractmethod
from typing import Dict, Generic, Optional, TypeVar
from uuid import uuid4

from fbpcs.common.service.simple_trace_logging_service import SimpleTraceLoggingService

from fbpcs.common.service.trace_logging_service import TraceLoggingService

R = TypeVar("R")


class RegistryFactory(ABC, Generic[R]):
    _DEFAULT_KEY: str = "RegistryFactoryDefaultKey"

    @classmethod
    def register_object(cls, key: str, value: R) -> None:
        cls._REGISTRY[key] = value

    @classmethod
    def get(cls, key: Optional[str] = None) -> R:
        # get the value associated with the key or the default (if the default is set)
        key = key or cls._DEFAULT_KEY
        val = cls._REGISTRY.get(key or cls._DEFAULT_KEY)
        if val:
            return val

        # get or register the default
        val = cls._REGISTRY.get(cls._DEFAULT_KEY)
        if not val:
            val = cls._get_default_value()
            cls.register_object(cls._DEFAULT_KEY, val)

        # set the key equal to the default
        cls.register_object(key, val)
        return val

    @classmethod
    def is_default_value(cls, val: R) -> bool:
        return val == cls.get()

    @classmethod
    @abstractmethod
    def _get_default_value(cls) -> R:
        raise NotImplementedError

    @classmethod
    @property
    @abstractmethod
    def _REGISTRY(cls) -> Dict[str, R]:
        raise NotImplementedError


class InstanceIdtoRunIdRegistry(RegistryFactory[str]):
    _REGISTRY: Dict[str, str] = {}

    @classmethod
    def _get_default_value(cls) -> str:
        return f"{uuid4()}-fbpcs-default"


class TraceLoggingRegistry(RegistryFactory[TraceLoggingService]):
    """This class will be used to get and store globally available trace loggers"""

    _REGISTRY: Dict[str, TraceLoggingService] = {}

    @classmethod
    def _get_default_value(cls) -> TraceLoggingService:
        return SimpleTraceLoggingService()
