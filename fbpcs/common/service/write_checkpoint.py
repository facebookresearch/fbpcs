# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import functools
import inspect
from contextlib import nullcontext, suppress
from typing import Any, Callable, ContextManager, Dict, List, Optional

from fbpcs.common.service.trace_logging_registry import (
    InstanceIdtoRunIdRegistry,
    TraceLoggingRegistry,
)

from fbpcs.common.service.trace_logging_service import TraceLoggingService


class write_checkpoint:
    _DEFAULT_REGISTRY_KEY = "checkpoint_key"
    _PARAMS_CONTAINING_INSTANCE_ID: List[str] = []
    _DEFAULT_COMPONENT_NAME = ""

    def __init__(
        self,
        *,
        instance_id_param: Optional[str] = None,
        dump_params: bool = False,
        dump_return_val: bool = False,
        # if include is not specified, only explicitly included kwarg values
        # will be dumped
        include: Optional[List[str]] = None,
        checkpoint_name: Optional[str] = None,
        checkpoint_data: Optional[Dict[str, str]] = None,
        component: Optional[str] = None,
        logging_svc_registry_key: Optional[str] = None,
        run_id_registry_key: Optional[str] = None,
    ) -> None:
        self.instance_id_param = instance_id_param
        self.dump_params = dump_params
        self.dump_return_val = dump_return_val
        self.include = include
        self.checkpoint_name = checkpoint_name
        self.checkpoint_data: Dict[str, str] = checkpoint_data or {}
        if component:
            self.checkpoint_data["component"] = component

        self._logging_svc_registry_key: str = (
            logging_svc_registry_key or self._DEFAULT_REGISTRY_KEY
        )
        self._run_id_registry_key: str = (
            run_id_registry_key or self._DEFAULT_REGISTRY_KEY
        )

    def __call__(self, func: Callable) -> Callable:  # pyre-ignore
        @functools.wraps(func)
        async def wrapper_async(*args: Any, **kwargs: Any) -> Any:  # pyre-ignore
            with self._get_trace_logger_cm(func, *args, **kwargs) as checkpoint_data:
                try:
                    res = await func(*args, **kwargs)
                except Exception as ex:
                    checkpoint_data["exception"] = str(ex)
                    raise ex
                if self.dump_return_val:
                    checkpoint_data["return_value"] = str(res)
                return res

        @functools.wraps(func)
        def wrapper_sync(*args: Any, **kwargs: Any) -> Any:  # pyre-ignore
            with self._get_trace_logger_cm(func, *args, **kwargs) as checkpoint_data:
                try:
                    res = func(*args, **kwargs)
                except Exception as ex:
                    checkpoint_data["exception"] = str(ex)
                    raise ex
                if self.dump_return_val:
                    checkpoint_data["return_value"] = str(res)
                return res

        if asyncio.iscoroutinefunction(func):
            return wrapper_async
        else:
            return wrapper_sync

    def _get_trace_logger_cm(
        self,
        func: Callable,  # pyre-ignore
        *args: Any,
        **kwargs: Dict[str, Any],
    ) -> ContextManager[Dict[str, str]]:
        ctx = nullcontext({})
        with suppress(Exception):
            function_args = inspect.signature(func).bind(*args, **kwargs).arguments
            instance_id = self._get_instance_id(function_args)
            run_id = self._instance_id_to_run_id(instance_id)
            trace_logging_svc = self._get_trace_logging_svc(function_args)
            checkpoint_data = self._get_checkpoint_data(function_args)
            checkpoint_name = self._get_checkpoint_name(func)
            ctx = trace_logging_svc.write_checkpoint_cm(
                run_id=run_id,
                instance_id=instance_id,
                checkpoint_name=checkpoint_name,
                checkpoint_data=checkpoint_data,
            )
        return ctx

    def _get_checkpoint_name(self, func: Callable) -> str:  # pyre-ignore
        return self.checkpoint_name or func.__name__.upper()

    def _get_component(self, function_args: Dict[str, Any]) -> str:
        if kls_arg := function_args.get("cls"):
            return kls_arg.__name__
        elif self_arg := function_args.get("self"):
            return self_arg.__class__.__name__
        else:
            return self._DEFAULT_COMPONENT_NAME

    def _get_trace_logging_svc(
        self, function_args: Dict[str, Any]
    ) -> TraceLoggingService:
        if kls_arg := function_args.get("cls"):
            trace_logging_svc = getattr(kls_arg, "TRACE_LOGGING_SVC", None) or getattr(
                kls_arg, "TRACE_LOGGING_SERVICE", None
            )
        elif self_arg := function_args.get("self"):
            trace_logging_svc = getattr(self_arg, "trace_logging_svc", None) or getattr(
                self_arg, "trace_logging_service", None
            )
        else:
            trace_logging_svc = None

        if not isinstance(trace_logging_svc, TraceLoggingService):
            trace_logging_svc = None

        return trace_logging_svc or self._get_default_trace_logger()

    def _get_checkpoint_data(self, kwargs: Dict[str, Any]) -> Dict[str, str]:
        checkpoint_data = self.checkpoint_data.copy()
        checkpoint_data["component"] = checkpoint_data.get(
            "component", self._get_component(kwargs)
        )

        if not self.dump_params:
            return checkpoint_data

        include = self.include or [
            key for key in kwargs.keys() if key not in ("cls", "self")
        ]
        checkpoint_data.update({param: str(kwargs.get(param)) for param in include})
        return checkpoint_data

    def _get_instance_id(self, kwargs: Dict[str, Any]) -> str:
        if id_param := self.instance_id_param:
            if instance_id := self._param_to_instance_id(id_param, kwargs):
                return instance_id
        else:
            for id_param in self._PARAMS_CONTAINING_INSTANCE_ID:
                if instance_id := self._param_to_instance_id(id_param, kwargs):
                    return instance_id
        return ""  # no instance id found

    @classmethod
    def _param_to_instance_id(
        cls, instance_id_param: str, kwargs: Dict[str, Any]
    ) -> Optional[str]:
        instance_id_obj = kwargs.get(instance_id_param)
        if isinstance(instance_id_obj, str):
            return instance_id_obj
        else:
            return None

    @classmethod
    def register_trace_logger(
        cls, trace_logging_service: TraceLoggingService, *, key: Optional[str] = None
    ) -> None:
        key = key or cls._DEFAULT_REGISTRY_KEY
        TraceLoggingRegistry.register_object(key, trace_logging_service)

    @classmethod
    def register_run_id(
        cls, run_id: Optional[str], *, key: Optional[str] = None
    ) -> str:
        run_id = run_id or InstanceIdtoRunIdRegistry.get()
        key = key or cls._DEFAULT_REGISTRY_KEY
        InstanceIdtoRunIdRegistry.register_object(key, run_id)
        return run_id

    def _get_default_trace_logger(self) -> TraceLoggingService:
        return TraceLoggingRegistry.get(self._logging_svc_registry_key)

    def _instance_id_to_run_id(self, instance_id: str) -> Optional[str]:
        run_id = InstanceIdtoRunIdRegistry.get(instance_id)
        if InstanceIdtoRunIdRegistry.is_default_value(run_id):
            run_id = InstanceIdtoRunIdRegistry.get(self._run_id_registry_key)
        return run_id
