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
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Type, TypeVar

from dataclasses_json import DataClassJsonMixin

T = TypeVar("T", bound="InstanceBase")

from fbpcs.common.entity.exceptions import (
    InstanceDeserializationError,
    InstanceVersionMismatchError,
    InstanceFrozenFieldError,
)
from fbpcs.common.entity.instance_base_config import (
    IS_FROZEN_FIELD_DEFAULT_VALUE,
    IS_FROZEN_FIELD_METADATA_STR,
    InstanceBaseMetadata,
)


@dataclass
class InstanceBase(DataClassJsonMixin):
    """Base class for all your PCS instance needs

    Provides json serde and versioning out of the box.

    Public attributes:
        version_hash: hash for instance schema. If schema changes, hash changes
        dirty: boolean that indicates if schema has changed since serialization
        created_ts: unixtime at which the instance was created
    """

    # ignored by constructor
    version_hash: str = field(init=False, metadata=InstanceBaseMetadata.IMMUTABLE)
    # ignored by constructor
    dirty: bool = field(init=False, metadata=InstanceBaseMetadata.MUTABLE)
    # ignored by constructor
    created_ts: int = field(init=False, metadata=InstanceBaseMetadata.IMMUTABLE)

    def __post_init__(self) -> None:
        self.version_hash = self.generate_version_hash()
        self.dirty = False
        self.created_ts = int(time.time())

    # TODO(T108616043): [PCS][BE] delete get_instance_id; make instance_id required field
    @abc.abstractmethod
    def get_instance_id(self) -> str:
        pass

    def __str__(self) -> str:
        return self.dumps_schema()

    # pyre-ignore Missing parameter annotation [2]
    def __setattr__(self, name: str, value: Any) -> None:
        """Override setattr to not change fields marked as frozen"""
        # if field already has been set and it is marked as frozen...
        # pyre-fixme Undefined attribute [16]: InstanceBase has no attribute __dataclass_fields__
        if name in self.__dict__ and self.__dataclass_fields__[name].metadata.get(
            IS_FROZEN_FIELD_METADATA_STR, IS_FROZEN_FIELD_DEFAULT_VALUE
        ):
            raise InstanceFrozenFieldError(
                f"Cannot change value of {name} because it is marked as an immutable field"
            )
        # the field has either never been set or is marked as mutable
        super().__setattr__(name, value)

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

    def _load_non_init_field(
        self,
        field_name: str,
        # pyre-ignore Missing parameter annotation [2]
        default_value: Any,
        instance_json_dict: Dict[str, Any],
    ) -> None:
        """Reads non-init field from instance dict and sets it on instance

        Arguments:
            field_name: name of the field to load into the instance
            default value: value to assign the field if none is in the json dict
            instance_json_dict: a dictionary rendering of a json serialized instance
        """
        super().__setattr__(
            field_name, instance_json_dict.get(field_name, default_value)
        )

    def _loads_non_init_fields(self, instance_json_dict: Dict[str, Any]) -> None:
        """Reads non-init fields from instance dict and sets them on instance

        Arguments:
            instance_json_dict: a dictionary rendering of a json serialized instance
        """
        self._load_non_init_field("version_hash", "", instance_json_dict)
        # if dirty field DNE in json, then instance is old (and thus is dirty)
        self._load_non_init_field("dirty", True, instance_json_dict)
        self._load_non_init_field("created_ts", 0, instance_json_dict)

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
