#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import atexit
import json
from itertools import groupby
from operator import itemgetter
from queue import Empty, SimpleQueue
from threading import Thread
from time import sleep
from typing import Dict, Optional

import requests

from fbpcs.common.service.secret_scrubber import SecretScrubber
from fbpcs.common.service.trace_logging_service import (
    CheckpointStatus,
    TraceLoggingService,
)


# Allow a request to be open for this many seconds before
# raising a requests.exceptions.Timeout exception.
# From the requests documentation: It’s a good practice to set
# connect timeouts to slightly larger than a multiple of 3,
# which is the default TCP packet retransmission window.
RESPONSE_TIMEOUT: float = 3.05
AGGREGATE_DELIMITER = r"\001"
FLUSH_CHUNK_SIZE = 10

DEFAULT_COMPONENT_NAME = "pcservice"


class GraphApiTraceLoggingService(TraceLoggingService):
    def __init__(self, access_token: str, endpoint_url: str) -> None:
        super().__init__()
        self.access_token = access_token
        self.endpoint_url = endpoint_url
        self.scrubber = SecretScrubber()
        self.msg_queue: SimpleQueue[Dict[str, Optional[str]]] = SimpleQueue()
        self.logger.info(f"Starting {self.__class__.__name__} background task...")
        daemon = Thread(
            target=self.background_task,
            args=(self.msg_queue,),
            daemon=True,
            name=self.__class__.__name__,
        )
        daemon.start()
        # register the at exit to flush msg_queue
        atexit.register(self.stop_background_task, self.msg_queue)

    # pyre-ignore: https://github.com/python/cpython/issues/99509
    def background_task(self, msg_queue) -> None:
        # run forever as daemon thread
        while True:
            if msg_queue.qsize() >= FLUSH_CHUNK_SIZE:
                self._flush_msg_queue(msg_queue=msg_queue, flush_size=FLUSH_CHUNK_SIZE)
            sleep(1)

    # pyre-ignore: https://github.com/python/cpython/issues/99509
    def stop_background_task(self, msg_queue) -> None:
        self._flush_msg_queue(msg_queue=msg_queue, flush_size=msg_queue.qsize())

    # pyre-ignore: https://github.com/python/cpython/issues/99509
    def _flush_msg_queue(self, msg_queue, flush_size: int = FLUSH_CHUNK_SIZE) -> None:
        msg_list = []
        for _ in range(flush_size):
            try:
                msg_list.append(msg_queue.get(block=False))
            except Empty:
                self.logger.warn(f"no message to flush in {self.__class__.__name__}")
                break

        # sort messages data by instance_id
        msg_list = sorted(msg_list, key=itemgetter("instance_id"))
        # group by instance_id with same post request
        for _, group_mgs_list in groupby(msg_list, key=itemgetter("instance_id")):
            aggregate_msg = {}
            for msg in group_mgs_list:
                if not aggregate_msg:
                    aggregate_msg = msg.copy()
                    continue

                aggregate_msg["component"] += f"{AGGREGATE_DELIMITER}{msg['component']}"
                aggregate_msg[
                    "checkpoint_name"
                ] += f"{AGGREGATE_DELIMITER}{msg['checkpoint_name']}"
                aggregate_msg[
                    "checkpoint_data"
                ] += f"{AGGREGATE_DELIMITER}{msg['checkpoint_data']}"

            if aggregate_msg:
                self._post_request(params=aggregate_msg)

    def _write_checkpoint_impl(
        self,
        run_id: Optional[str],
        instance_id: str,
        checkpoint_name: str,
        status: CheckpointStatus,
        checkpoint_data: Optional[Dict[str, str]] = None,
    ) -> None:

        checkpoint_data = checkpoint_data or {}
        component = checkpoint_data.pop("component", DEFAULT_COMPONENT_NAME)
        scrubbed_checkpoint_data = {}
        for k, v in checkpoint_data.items():
            scrubbed_key = self.scrubber.scrub(k).scrubbed_output
            scrubbed_val = self.scrubber.scrub(v).scrubbed_output
            scrubbed_checkpoint_data[scrubbed_key] = scrubbed_val

        params = {
            "run_id": run_id,
            "instance_id": instance_id,
            "checkpoint_name": f"{checkpoint_name}_{status}",
            "access_token": self.access_token,
            "component": component,
            "checkpoint_data": json.dumps(checkpoint_data),
        }

        # put checkpoint into msg queue, allow daemon to take care of it
        self.msg_queue.put(params)

        log_data = params.copy()
        del log_data["access_token"]

        # We run the secret scrubber since we want to be completely
        # sure we don't accidentally log an access token
        log_dump = json.dumps(log_data)
        scrubbed_log_dump = self.scrubber.scrub(log_dump).scrubbed_output
        self.logger.debug(scrubbed_log_dump)

    def _post_request(self, params: Dict[str, Optional[str]]) -> None:
        res_info = {}
        try:
            r = requests.post(
                self.endpoint_url, params=params, timeout=RESPONSE_TIMEOUT
            )
            res_info["requests_post_status_code"] = str(r.status_code)
            res_info["requests_post_reason"] = str(r.reason)
        except requests.exceptions.Timeout:
            res_info["extra_info"] = "Timeout reaching endpoint"
        except Exception as e:
            res_info["extra_info"] = f"Unexpected error: {e}"

        self.logger.info(f"post checkpoints : {res_info}")
