#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import unittest
from dataclasses import dataclass

from fbpcs.common.entity.exceptions import InstanceVersionMismatchError
from fbpcs.common.entity.instance_base import InstanceBase


@dataclass
class DummyInstanceV1(InstanceBase):
    """Dummy instance class to be used in unit tests"""

    instance_id: str

    def get_instance_id(self) -> str:
        return self.instance_id


@dataclass
class DummyInstanceV2(InstanceBase):
    """Dummy instance class to be used in unit tests"""

    instance_id: str
    new_field: str = "default"

    def get_instance_id(self) -> str:
        return self.instance_id


class TestInstanceBase(unittest.TestCase):
    def test_basic_serde(self):
        """Test that serialization followed by deserialization results in a replica"""

        instance = DummyInstanceV1(instance_id="123")
        serialized_instance = instance.dumps_schema()
        deserialized_instance = DummyInstanceV1.loads_schema(serialized_instance)
        self.assertEqual(instance, deserialized_instance)

        # The schema never changed, so the instance should not be dirty
        self.assertFalse(instance.dirty)

    def test_dirty_deserialization_non_strict_new_hash(self):
        """Test that dirty flag is set if instance schema changes"""

        # version hashes should be different
        self.assertNotEqual(
            DummyInstanceV1.generate_version_hash(),
            DummyInstanceV2.generate_version_hash(),
        )

        instance = DummyInstanceV1(instance_id="123")
        # a never serialized instance should not be dirty
        self.assertFalse(instance.dirty)

        serialized_instance = instance.dumps_schema()
        deserialized_instance = DummyInstanceV2.loads_schema(
            serialized_instance, strict=False
        )

        # since the schema has changed, the instance should be marked dirty
        self.assertTrue(deserialized_instance.dirty)

        # we keep the original version hash, even though the schema has changed
        self.assertEqual(instance.version_hash, deserialized_instance.version_hash)

    def test_dirty_deserialization_non_strict_previously_dirty(self):
        """Test that a dirty flag always persists through subsequent serde"""

        instance = DummyInstanceV1(instance_id="123")
        instance.dirty = True
        serialized_instance = instance.dumps_schema()
        deserialized_instance = DummyInstanceV1.loads_schema(serialized_instance)
        self.assertEqual(instance, deserialized_instance)

        # Sanity check: The schema never changed but it was marked dirty at some point, so it should remain dirty
        self.assertTrue(instance.dirty)

    def test_dirty_deserialization_strict(self):
        """Test that an exception is thrown if strict mode is set"""

        instance = DummyInstanceV1(instance_id="123")

        serialized_instance = instance.dumps_schema()
        with self.assertRaises(InstanceVersionMismatchError):
            DummyInstanceV2.loads_schema(serialized_instance, strict=True)

        # strict won't result in exception if instance is not dirty
        DummyInstanceV1.loads_schema(serialized_instance, strict=True)
