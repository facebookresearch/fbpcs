#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import inspect
import itertools
from contextlib import nullcontext
from typing import Any, Callable, ContextManager, Dict, List
from unittest import IsolatedAsyncioTestCase

from fbpcs.common.service.simple_trace_logging_service import SimpleTraceLoggingService
from fbpcs.common.service.trace_logging_registry import InstanceIdtoRunIdRegistry
from fbpcs.common.service.trace_logging_service import TraceLoggingService

from fbpcs.common.service.write_checkpoint import write_checkpoint


class dummy_checkpoint(write_checkpoint):
    _DEFAULT_REGISTRY_KEY = "dummy_checkpoint_key"
    _PARAMS_CONTAINING_INSTANCE_ID: List[str] = [
        "instance_id",
    ]
    _DEFAULT_COMPONENT_NAME = "DefaultDummyComponent"

    def _get_trace_logger_cm(
        self,
        func: Callable,  # pyre-ignore
        *args: Any,
        **kwargs: Dict[str, Any],
    ) -> ContextManager[Dict[str, str]]:
        return nullcontext(self.checkpoint_data)


class DummyException(Exception):
    pass


def add(instance_id: str, x: int, y: int = 2) -> int:
    return x + y


def raise_exception(instance_id: str) -> None:
    raise DummyException("dummy_exception")


class DummyTraceLoggingService(SimpleTraceLoggingService):
    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name


class DummyClass:
    TRACE_LOGGING_SVC = DummyTraceLoggingService("class_default")
    _OBJ_TRACE_LOGGING_SVC = DummyTraceLoggingService("obj_default")

    def __init__(self) -> None:
        self.trace_logging_svc: TraceLoggingService = self._OBJ_TRACE_LOGGING_SVC

    def add(self, instance_id: str, x: int, y: int) -> int:
        return x + y

    @classmethod
    def subtract(cls, x: int, y: int) -> int:
        return x - y

    async def async_add(self, instance_id: str, x: int, y: int) -> int:
        return x + y


class TestWriteCheckpoint(IsolatedAsyncioTestCase):
    async def test_basic_usage(self) -> None:
        # test that the decorator doesn't affect results
        with self.subTest("static func"):
            res = dummy_checkpoint()(add)("instance_id", 1, 2)
            self.assertEqual(res, 3)

        with self.subTest("method"):
            obj = DummyClass()
            res = dummy_checkpoint()(obj.add)("instance_id", 1, 2)
            self.assertEqual(res, 3)

        with self.subTest("class method"):
            res = dummy_checkpoint()(DummyClass.subtract)(1, 2)
            self.assertEqual(res, -1)

        with self.subTest("raise exception"):
            checkpoint = dummy_checkpoint()
            with self.assertRaises(DummyException):
                checkpoint(raise_exception)("instance_id")

            self.assertEqual(
                checkpoint.checkpoint_data, {"exception": "dummy_exception"}
            )

        with self.subTest("async add"):
            obj = DummyClass()
            res = await dummy_checkpoint()(obj.async_add)("instance_id", 1, 2)
            self.assertEqual(res, 3)

    async def test_dump_return_val(self) -> None:
        for dump_return_val in (True, False):
            with self.subTest(dump_return_value=dump_return_val):
                checkpoint_dec = dummy_checkpoint(dump_return_val=dump_return_val)
                res = checkpoint_dec(add)("instance", 1)
                self.assertEqual(res, 3)
                if dump_return_val:
                    self.assertEqual(
                        checkpoint_dec.checkpoint_data["return_value"], str(res)
                    )
                else:
                    self.assertNotIn("return_value", checkpoint_dec.checkpoint_data)

    def test_get_checkpoint_data(self) -> None:
        for (
            my_checkpoint_data,
            component_name,
            dump_params,
            include,
        ) in itertools.product(
            ({}, {"my_data": "my_data"}),
            ((None, "test component")),
            ((True, False)),
            ((None, ["x"])),
        ):
            with self.subTest(
                my_checkpoint_data=my_checkpoint_data,
                component_name=component_name,
                dump_params=dump_params,
                include=include,
            ):
                kwargs = {"instance_id": "instance", "x": 1, "y": 2}
                function_args = inspect.signature(add).bind(**kwargs).arguments
                checkpoint_data = dummy_checkpoint(
                    checkpoint_data=my_checkpoint_data,
                    component=component_name,
                    dump_params=dump_params,
                    include=include,
                )._get_checkpoint_data(function_args)
                if dump_params:
                    for k, v in kwargs.items():
                        if include and k not in include:
                            self.assertNotIn(k, checkpoint_data)
                        else:
                            self.assertEqual(checkpoint_data[k], str(v))
                else:
                    for k in kwargs:
                        self.assertNotIn(k, checkpoint_data)

                for k, v in my_checkpoint_data.items():
                    self.assertEqual(checkpoint_data[k], str(v))

                expected_component_name = (
                    component_name or dummy_checkpoint._DEFAULT_COMPONENT_NAME
                )

                self.assertEqual(checkpoint_data["component"], expected_component_name)

    def test_get_instance_id(self) -> None:
        for instance_id_param in (None, "instance_id"):
            with self.subTest(instance_id_param=instance_id_param):
                kwargs = {"instance_id": "instance", "x": 1, "y": 2}
                function_args = inspect.signature(add).bind(**kwargs).arguments
                instance_id = dummy_checkpoint(
                    instance_id_param=instance_id_param
                )._get_instance_id(function_args)

                self.assertEqual(kwargs["instance_id"], instance_id)

    def test_get_trace_logging_service(self) -> None:
        with self.subTest("default"):
            kwargs = {"instance_id": "instance", "x": 1, "y": 2}
            function_args = inspect.signature(add).bind(**kwargs).arguments
            trace_logging_svc = dummy_checkpoint()._get_trace_logging_svc(function_args)
            isinstance(trace_logging_svc, TraceLoggingService)

        with self.subTest("new default"):
            kwargs = {"instance_id": "instance", "x": 1, "y": 2}
            function_args = inspect.signature(add).bind(**kwargs).arguments
            checkpoint = dummy_checkpoint()
            new_svc = DummyTraceLoggingService("new service")
            dummy_checkpoint.register_trace_logger(new_svc)
            trace_logging_svc = checkpoint._get_trace_logging_svc(function_args)
            self.assertEqual(new_svc, trace_logging_svc)

        with self.subTest("custom registry key"):
            kwargs = {"instance_id": "instance", "x": 1, "y": 2}
            function_args = inspect.signature(add).bind(**kwargs).arguments
            checkpoint = dummy_checkpoint(logging_svc_registry_key="test key")
            new_svc = DummyTraceLoggingService("new service")
            dummy_checkpoint.register_trace_logger(new_svc, key="test key")
            trace_logging_svc = checkpoint._get_trace_logging_svc(function_args)
            self.assertEqual(new_svc, trace_logging_svc)

        with self.subTest("class method, stored on class"):
            kwargs = {"x": 1, "y": 2}
            function_args = (
                inspect.signature(DummyClass.subtract).bind(**kwargs).arguments
            )
            function_args["cls"] = DummyClass

            trace_logging_svc = dummy_checkpoint()._get_trace_logging_svc(function_args)
            self.assertEqual(trace_logging_svc, DummyClass.TRACE_LOGGING_SVC)

        with self.subTest("method, stored on obj"):
            kwargs = {"instance_id": "instance", "x": 1, "y": 2}
            obj = DummyClass()
            function_args = inspect.signature(obj.add).bind(**kwargs).arguments
            function_args["self"] = obj
            trace_logging_svc = dummy_checkpoint()._get_trace_logging_svc(function_args)
            self.assertEqual(trace_logging_svc, obj.trace_logging_svc)

    def test_instance_id_to_run_id(self) -> None:
        instance_id = "instance id"
        default_run_id = "12345"
        run_id_registry_key = "test key"
        InstanceIdtoRunIdRegistry.register_object(run_id_registry_key, default_run_id)

        with self.subTest("default run id, default registry key"):
            checkpoint = dummy_checkpoint()
            self.assertNotEqual(
                default_run_id, checkpoint._instance_id_to_run_id(instance_id)
            )

        with self.subTest("default run id, custom registry key"):
            checkpoint = dummy_checkpoint(run_id_registry_key=run_id_registry_key)
            self.assertEqual(
                default_run_id, checkpoint._instance_id_to_run_id(instance_id)
            )

        with self.subTest("matched instance id"):
            run_id = "hello"
            checkpoint = dummy_checkpoint()
            checkpoint.register_run_id(run_id, key=instance_id)
            self.assertEqual(run_id, checkpoint._instance_id_to_run_id(instance_id))
