#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import hashlib
import inspect
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Type, TypeVar

from dataclasses_json import DataClassJsonMixin

T = TypeVar("T", bound="InstanceBase")

from fbpcs.common.entity.exceptions import (
    InstanceDeserializationError,
    InstanceVersionMismatchError,
)


@dataclass
class InstanceBase(DataClassJsonMixin):
    """Base class for all your PCS instance needs

    Provides json serde and versioning out of the box.

    Public attributes:
        version_hash: hash for instance schema. If schema changes, hash changes
        dirty: boolean that indicates if schema has changed since serialization
    """

    # ignored by constructor
    version_hash: str = field(init=False)
    # ignored by constructor
    dirty: bool = field(init=False)

    def __post_init__(self) -> None:
        self.version_hash = self.generate_version_hash()
        self.dirty = False

    # TODO(T108616043): [PCS][BE] delete get_instance_id; make instance_id required field
    @abc.abstractmethod
    def get_instance_id(self) -> str:
        pass

    def __str__(self) -> str:
        return self.dumps_schema()

    def dumps_schema(self) -> str:
        """Serializes the instance, returns as a string"""
        return self.schema().dumps(self)

    @classmethod
    def generate_version_hash(cls: Type[T]) -> str:
        """
        1. Retrieve the source code for the class
        2. hash it

        If the class changes, the hash changes.
        """
        cls_source_code_bytes = inspect.getsource(cls).encode()
        return hashlib.md5(cls_source_code_bytes).hexdigest()

    def _loads_non_init_fields(self, instance_json_dict: Dict[str, Any]) -> None:
        """Reads non-init fields from instance dict and sets them on instance

        Arguments:
            instance_json_dict: a dictionary rendering of a json serialized instance
        """
        self.version_hash = instance_json_dict.get("version_hash", "")
        # if dirty field DNE in json, then instance is old (and thus is dirty)
        self.dirty = instance_json_dict.get("dirty", True)

    @classmethod
    def loads_schema(cls: Type[T], json_schema_str: str, strict: bool = False) -> T:
        """Deserializes an instance.

        Arguments:
            json_schema_str: the serialized instance string
            strict: should an error be thrown if the instance is dirty?

        Raises:
            InstanceVersionMismatchError: thrown if schema version changed (instance is dirty)
            InstanceDeserializationError: Catch-all exception for unexpected errors

        Returns:
            A deserialized instance of type T (same as the calling cls)
        """
        try:
            # deserializes instance
            instance = cls.schema().loads(json_schema_str, many=None)

            # some fields are ignored by json-dataclasses deserializer because init=False
            # manually load them
            instance._loads_non_init_fields(json.loads(json_schema_str))

            # if the instance was previously dirty or version hash has changed, mark instance as dirty
            instance.dirty = (
                instance.dirty or instance.version_hash != cls.generate_version_hash()
            )
            if instance.dirty:
                instance_id = instance.get_instance_id()
                if strict:
                    raise InstanceVersionMismatchError(
                        f"{cls.__name__} dataclass schema version has changed since initial serialization of {instance_id=}."
                    )
                else:
                    logging.warning(
                        f"Instance dataclass schema for {cls.__name__} has changed since initial serialization of {instance_id=}!"
                    )
            return instance
        except InstanceVersionMismatchError:
            raise
        except Exception as e:
            raise InstanceDeserializationError(str(e)) from e
