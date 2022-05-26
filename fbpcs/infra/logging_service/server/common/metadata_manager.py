# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import threading
import time

from fbpcs.infra.logging_service.server.common.data_model import MetadataEntity
from fbpcs.infra.logging_service.server.common.logging_client import LoggingClient
from fbpcs.infra.logging_service.server.common.queue_manager import QueueManager


# Manager for log metadata, e.g. uploading to backend.
class MetadataManager:
    SLEEP_INTERVAL_SECOND = 1
    UPLOAD_BATCH_SIZE = 5

    def __init__(
        self, queue_manager: QueueManager, logging_client: LoggingClient
    ) -> None:
        self.logger: logging.Logger = logging.getLogger()
        self.queue_manager = queue_manager
        self.logging_client = logging_client
        self._start_upload()

    def put_metadata(
        self,
        partner_id: str,
        entity_key: str,
        entity_value: str,
    ) -> None:
        self.logger.info(f"put_metadata: entity_key={entity_key}.")
        queue_entity = MetadataEntity(partner_id, entity_key, entity_value)
        self.queue_manager.add_metadata(queue_entity)

    def _start_upload(
        self,
    ) -> None:
        threading.Thread(target=self._process_upload_queue_thread, name=None).start()

    def _process_upload_queue_thread(
        self,
    ) -> None:
        while True:
            entities = self.queue_manager.peek_metadata(self.UPLOAD_BATCH_SIZE)
            if entities:
                count_put_success = 0
                for entity in entities:
                    try:
                        self.logging_client.put_metadata(entity)
                        count_put_success += 1
                    except Exception as ex:
                        self.logger.error(
                            f"process_upload_queue: error in logging_client.put_metadata: {str(ex)}."
                        )
                        break

                self.queue_manager.remove_metadata(count_put_success)

            time.sleep(self.SLEEP_INTERVAL_SECOND)
