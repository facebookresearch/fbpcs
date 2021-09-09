#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import abc
import dataclasses
import time
from typing import Any, Dict, Optional

from fbpcp.service.storage import StorageService

# Default time to wait between checking for a coordination object
SLEEP_INTERVAL_SECS = 5


class CoordinationObjectAlreadyExistsError(ValueError):
    def __init__(self, key: str, *args: Any):
        self.message = f"Object {key} already in coordination objects list"
        super().__init__(self, key, *args)


class MissingCoordinationObjectError(RuntimeError):
    def __init__(self, key: str, *args: Any):
        self.message = f"Missing required coordination object: {key}"
        super().__init__(self, key, *args)


@dataclasses.dataclass(frozen=True)
class CoordinationObject(object):
    value: str
    sleep_interval_secs: int = SLEEP_INTERVAL_SECS
    timeout_secs: Optional[int] = None


class CoordinationService(abc.ABC):
    coordination_objects: Dict[str, CoordinationObject]

    def __init__(
        self,
        coordination_objects: Dict[str, Dict[str, Any]],
        storage_svc: Optional[StorageService],
    ):
        self.storage_svc = storage_svc
        self.coordination_objects = {}
        for key, params in coordination_objects.items():
            self.add_coordination_object(key, params)

    def add_coordination_object(
        self,
        key: str,
        params: Dict[str, Any],
        raise_on_overwrite=True,
    ) -> CoordinationObject:
        """
        Add a new coordination object for tracking. This method is called from
        the constructor, but additional coordination objects may be added later.
        """
        if raise_on_overwrite and key in self.coordination_objects:
            raise CoordinationObjectAlreadyExistsError(key)
        res = CoordinationObject(**params)
        self.coordination_objects[key] = res
        return res

    def is_tracking(self, key: str) -> bool:
        """
        Check if a key is tracked within this Service's coordination objects
        """
        return key in self.coordination_objects

    def wait(self, key: str) -> bool:
        """
        Attempt to coordinate for the given coordination object key.
        This will block until the object timeout is reached. If timeout is None,
        it will block forever.
        """
        obj = self.coordination_objects[key]
        start = time.time()
        found = False
        while not found:
            found = self._is_coordination_object_ready(obj.value)
            elapsed = time.time() - start
            if found or (obj.timeout_secs and elapsed > obj.timeout_secs):
                break
            time.sleep(obj.sleep_interval_secs)
        return found

    def put_payload(self, key: str, data: Any) -> None:
        """
        Store some data in the given key's coordination object. It is up to the
        underlying concrete class to define any necessary data SerDe.
        """
        obj = self.coordination_objects[key]
        self._put_data(obj.value, data)

    def get_payload(self, key: str) -> Any:
        """
        Retrieve the data stored in the given key's coordination object. It is
        up to the underlying concrete class to define any necessary data SerDe.
        This is separate from `wait` since it may be preferable to differentiate
        between data not ready (`wait` returns False) and the stored payload
        actually containing `False`.
        """
        obj = self.coordination_objects[key]
        return self._get_data(obj.value)

    @abc.abstractmethod
    def _is_coordination_object_ready(self, value: str) -> bool:
        """
        Check if the given coordination object is ready right now.
        This is called from the public `wait` method which will handle looping
        and blocking as necessary.
        """
        pass

    @abc.abstractmethod
    def _put_data(self, value: str, data: Any) -> None:
        """
        Put `data` into the given coordination object. This is called from the
        public `signal` method which will handle blocking as necessary.
        """
        pass

    @abc.abstractmethod
    def _get_data(self, value: str) -> Any:
        """
        Retrieve data from the given coordination object. This is called from
        the public `get_payload` method which will handle blocking as necessary.
        """
        pass
