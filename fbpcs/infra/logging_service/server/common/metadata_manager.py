# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
import threading
import time

from common.data_model import MetadataEntity
from common.queue_manager import QueueManager
from common.remote_connector import RemoteConnector


# Manager for log metadata, e.g. uploading to backend.
class MetadataManager:
    SLEEP_INTERVAL_SECOND = 1
    UPLOAD_BATCH_SIZE = 5

    def __init__(self, queue_manager: QueueManager, remote_connector: RemoteConnector):
        self.logger = logging.getLogger()
        self.queue_manager = queue_manager
        self.remote_connector = remote_connector

    def put_metadata(
        self,
        partner_id: str,
        entity_key: str,
        entity_value: str,
    ) -> bool:
        self.logger.info(f"put_metadata: entity_key={entity_key}.")
        queue_entity = MetadataEntity(partner_id, entity_key, entity_value)
        self.queue_manager.add_metadata(queue_entity)
        return True

    def start_upload(
        self,
    ) -> None:
        threading.Thread(target=self._process_upload_queue_thread, name=None).start()

    def _process_upload_queue_thread(
        self,
    ) -> None:
        while True:
            entities = self.queue_manager.peek_metadata(self.UPLOAD_BATCH_SIZE)
            if entities:
                for entity in entities:
                    try:
                        self.remote_connector.put_metadata(entity)
                    except Exception as ex:
                        self.logger.error(
                            f"process_upload_queue: error in remote_connector.put_metadata: {str(ex)}."
                        )
                        raise

                self.queue_manager.remove_metadata(len(entities))

            time.sleep(self.SLEEP_INTERVAL_SECOND)
