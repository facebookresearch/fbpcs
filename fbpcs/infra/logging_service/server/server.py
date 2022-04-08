# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
import sys
import time
from typing import Dict, List, Optional

from meta.private_computation import LoggingService
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport
from meta.private_computation.ttypes import *
from common.data_model import MetadataEntity
from common.memory_queue_manager import MemoryQueueManager
from common.meta_remote_connector import MetaRemoteConnector
from common.metadata_manager import MetadataManager
from common.queue_manager import QueueManager
from common.remote_connector import RemoteConnector
from common.utils import Utils


logger = logging.getLogger()

# Handler for the logging service API's
class LoggingServiceHandler:
    def __init__(
        self, metadata_manager: MetadataManager, remote_connector: RemoteConnector
    ):
        self.logger = logging.getLogger()
        self.metadata_manager = metadata_manager
        self.remote_connector = remote_connector

    def putMetadata(self, request):
        """
        Each metadata entity will be queued in the metadata manager, and
        then uploaded to remote backend.
        """
        self.logger.info(f"putMetadata: request={request}.")
        success = self.metadata_manager.put_metadata(
            request.partner_id, request.entity_key, request.entity_value
        )
        return PutMetadataResponse(success)

    def getMetadata(self, request):
        """
        Metadata will be directly retrieved from the remote backend.
        """
        self.logger.info(f"getMetadata: request={request}.")
        entity_value = self.remote_connector.get_metadata(
            request.partner_id, request.entity_key
        )
        return GetMetadataResponse(entity_value)

    def listMetadata(self, request):
        """
        Metadata will be directly retrieved from the remote backend.
        """
        self.logger.info(f"listMetadata: request={request}.")
        key_values = self.remote_connector.list_metadata(
            request.partner_id,
            request.entity_key_start,
            request.entity_key_end,
            request.result_limit,
        )
        return ListMetadataResponse(key_values)


def main() -> None:
    Utils.configure_logger(logger, "log/server.log")

    queue_manager = MemoryQueueManager()
    remote_connector = MetaRemoteConnector()
    metadata_manager = MetadataManager(queue_manager, remote_connector)
    metadata_manager.start_upload()
    handler = LoggingServiceHandler(metadata_manager, remote_connector)
    processor = LoggingService.Processor(handler)
    transport = TSocket.TServerSocket(None, port=Utils.get_server_port())
    # transport = TSocket.TServerSocket("127.0.0.1", port=9090)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    # https://github.com/apache/thrift/blob/master/lib/py/src/server/TServer.py
    # Server with a fixed size pool of threads which service requests.
    server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
    server.setNumThreads(8)

    logger.info("Starting the server...")
    server.serve()
    logger.info("done.")


if __name__ == "__main__":
    main()
