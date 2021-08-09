#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
from unittest.mock import Mock, patch

from fbpmp.pid.service.coordination.coordination import (
    CoordinationObject,
    CoordinationObjectAlreadyExistsError,
    CoordinationService,
)


class TestCoordinationService(unittest.TestCase):
    @patch.object(CoordinationService, "__abstractmethods__", set())
    def test_add_coordination_object(self):
        svc = CoordinationService({}, storage_svc=None)
        self.assertEqual(len(svc.coordination_objects), 0)
        # Test default add
        params = {"value": "value"}
        res = svc.add_coordination_object("key1", params=params)
        self.assertEqual(res, CoordinationObject("value"))

        # Test add with parameters
        params = {"value": "value2", "timeout_secs": 123}
        res = svc.add_coordination_object("key2", params=params)
        self.assertEqual(res.value, "value2")
        self.assertEqual(res.timeout_secs, 123)

        # Test adding a key that already exists
        with self.assertRaises(CoordinationObjectAlreadyExistsError):
            svc.add_coordination_object("key1", params=params)

    @patch.object(CoordinationService, "__abstractmethods__", set())
    def test_is_tracking(self):
        svc = CoordinationService({}, storage_svc=None)
        self.assertFalse(svc.is_tracking("key1"))

        params = {"value": "value"}
        svc.add_coordination_object("key1", params=params)
        self.assertTrue(svc.is_tracking("key1"))

    @patch.object(CoordinationService, "__abstractmethods__", set())
    @patch("time.sleep", return_value=None)
    def test_wait(self, mock_time_sleep):
        params = {"value": "value", "sleep_interval_secs": 99, "timeout_secs": 100}
        objs = {"key1": params}
        svc = CoordinationService(objs, None)

        # Test that we call until True is returned
        svc._is_coordination_object_ready = Mock(side_effect=[False, True])
        with patch("time.time", return_value=0) as m:
            res = svc.wait("key1")
            self.assertTrue(res)
            # 3 calls: start, first elapsed (found=False), second elapsed (found=True)
            self.assertEqual(m.call_count, 3)

        # Test that the timeout works properly
        svc._is_coordination_object_ready = Mock(return_value=False)
        with patch("time.time", side_effect=[0, 0, 99, 188]) as m:
            res = svc.wait("key1")
            self.assertFalse(res)
            # 4 calls: start, first elapsed, second elapsed, third elapsed
            self.assertEqual(m.call_count, 4)

    @patch.object(CoordinationService, "__abstractmethods__", set())
    def test_put_payload(self):
        params = {"value": "value1"}
        objs = {"key1": params}
        svc = CoordinationService(objs, None)

        # Test default behavior
        svc._put_data = Mock()
        data = "hello world"
        svc.put_payload("key1", data)
        svc._put_data.assert_called_once_with("value1", data)

        # Test with missing key
        with self.assertRaises(KeyError):
            svc.put_payload("bad key", data)

    @patch.object(CoordinationService, "__abstractmethods__", set())
    def test_get_payload(self):
        params = {"value": "value1"}
        objs = {"key1": params}
        svc = CoordinationService(objs, None)

        # Test default behavior
        data = "hello world"
        svc._get_data = Mock(return_value=data)
        res = svc.get_payload("key1")
        svc._get_data.assert_called_once_with("value1")
        self.assertEqual(res, data)

        # Test with missing key
        with self.assertRaises(KeyError):
            svc.get_payload("bad key")
