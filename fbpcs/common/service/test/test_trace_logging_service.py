#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

from typing import Dict
from unittest import TestCase

from fbpcs.common.service.trace_logging_service import (
    CheckpointStatus,
    TraceLoggingService,
)


class DummyTraceLoggingService(TraceLoggingService):
    def __init__(self, fail_to_log: bool = False) -> None:
        self.events = []
        self.fail_to_log = fail_to_log

    # pyre-ignore
    def _write_checkpoint_impl(
        self,
        **kwargs,
    ) -> None:
        if self.fail_to_log:
            raise RuntimeError("Failing to log for test")
        self.events.append(kwargs)

    def _extract_caller_info(self) -> Dict[str, str]:
        return {"caller_info": "caller_info"}

    def _extract_error_info(self) -> Dict[str, str]:
        return {"error_info": "error_info"}


class TestTraceLoggingService(TestCase):
    def test_write_checkpoint_simple(self) -> None:
        for checkpoint_data in (None, {}, {"my_data": "my_data"}):
            for status in CheckpointStatus:
                with self.subTest(checkpoint_data=checkpoint_data, status=status):
                    svc = DummyTraceLoggingService()
                    payload = {
                        "run_id": "run123",
                        "instance_id": "instance456",
                        "checkpoint_name": "foo",
                        "status": CheckpointStatus.STARTED,
                        "checkpoint_data": checkpoint_data,
                    }
                    # pyre-ignore
                    svc.write_checkpoint(**payload.copy())

                    checkpoint_data = checkpoint_data or {}
                    checkpoint_data["caller_info"] = "caller_info"
                    if status is CheckpointStatus.FAILED:
                        checkpoint_data["error_info"] = "error_info"

                    payload["checkpoint_data"] = checkpoint_data
                    self.assertEqual(payload, svc.events[0])

    def test_write_checkpoint_cm(self) -> None:
        for checkpoint_data in (None, {}, {"my_data": "my_data"}):
            for bad_function_payload in (False, True):
                for fail_to_log in (False, True):
                    with self.subTest(
                        checkpoint_data=checkpoint_data,
                        bad_function_payload=bad_function_payload,
                        fail_to_log=fail_to_log,
                    ):
                        svc = DummyTraceLoggingService(fail_to_log=fail_to_log)
                        payload = {
                            "run_id": "run123",
                            "instance_id": "instance456",
                            "checkpoint_name": "foo",
                            "checkpoint_data": (
                                checkpoint_data.copy() if checkpoint_data else None
                            ),
                        }

                        try:
                            with svc.write_checkpoint_cm(
                                # pyre-ignore
                                **payload
                            ) as cm:
                                cm["before_failure"] = "before_failure"
                                assert bad_function_payload is False
                                cm["after_failure"] = "after_failure"
                        except AssertionError:
                            self.assertTrue(bad_function_payload)
                            expected_payload2 = {
                                **payload,
                                "status": CheckpointStatus.FAILED,
                                "checkpoint_data": {
                                    **(checkpoint_data or {}),
                                    "before_failure": "before_failure",
                                    "error_info": "error_info",
                                },
                            }
                        else:
                            expected_payload2 = {
                                **payload,
                                "status": CheckpointStatus.COMPLETED,
                                "checkpoint_data": {
                                    **(checkpoint_data or {}),
                                    "before_failure": "before_failure",
                                    "after_failure": "after_failure",
                                },
                            }
                        finally:
                            if fail_to_log:
                                self.assertEqual(len(svc.events), 0)
                            else:
                                self.assertEqual(len(svc.events), 2)
                                started_payload = {
                                    **payload,
                                    "status": CheckpointStatus.STARTED,
                                    "checkpoint_data": checkpoint_data or {},
                                }
                                self.assertEqual(started_payload, svc.events[0])

                                actual_payload = svc.events[1]
                                del actual_payload["checkpoint_data"]["runtime_ms"]
                                self.assertEqual(actual_payload, expected_payload2)
