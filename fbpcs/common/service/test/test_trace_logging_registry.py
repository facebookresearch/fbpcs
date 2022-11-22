#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from typing import Dict
from unittest import TestCase

from fbpcs.common.service.trace_logging_registry import RegistryFactory


class DummyRegistry(RegistryFactory[str]):

    _REGISTRY: Dict[str, str] = {}

    @classmethod
    def _get_default_value(cls) -> str:
        return "default value"


class TestRegistryFactory(TestCase):
    def test_registry(self) -> None:
        expected_registry = {}
        for key in [None, "key1", DummyRegistry._DEFAULT_KEY]:
            for register_val in [None, "val1"]:
                start_registry = expected_registry.copy()
                if register_val and key:
                    expected_value = register_val
                else:
                    expected_value = DummyRegistry._get_default_value()

                expected_registry[key or DummyRegistry._DEFAULT_KEY] = expected_value
                with self.subTest(
                    key=key,
                    register_val=register_val,
                    expected_value=expected_value,
                    start_registry=start_registry,
                    expected_registry=expected_registry,
                ):
                    self.assertEqual(DummyRegistry._REGISTRY, start_registry)

                    if register_val and key:
                        DummyRegistry.register_object(key, register_val)

                    actual_value = DummyRegistry.get(key)
                    self.assertEqual(actual_value, expected_value)

                    self.assertEqual(DummyRegistry._REGISTRY, expected_registry)
