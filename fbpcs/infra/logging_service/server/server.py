# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging

from common.logging_client import LoggingClient
from common.memory_queue_manager import MemoryQueueManager
from common.meta_logging_client import MetaLoggingClient
from common.metadata_manager import MetadataManager
from common.utils import Utils
from meta.private_computation import LoggingService
from meta.private_computation.ttypes import (
    GetMetadataResponse,
    ListMetadataResponse,
    PutMetadataResponse,
)
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport


global_logger = logging.getLogger()

# Handler for the logging service API's
class LoggingServiceHandler:
    def __init__(
        self, metadata_manager: MetadataManager, logging_client: LoggingClient
    ):
        self.logger = logging.getLogger()
        self.metadata_manager = metadata_manager
        self.logging_client = logging_client

    def putMetadata(self, request):
        """
        Each metadata entity will be queued in the metadata manager, and
        then uploaded to remote backend.
        """
        self.logger.info(f"putMetadata: request={request}.")
        self.metadata_manager.put_metadata(
            request.partner_id, request.entity_key, request.entity_value
        )
        return PutMetadataResponse()

    def getMetadata(self, request):
        """
        Metadata will be directly retrieved from the remote backend.
        """
        self.logger.info(f"getMetadata: request={request}.")
        entity_value = self.logging_client.get_metadata(
            request.partner_id, request.entity_key
        )
        return GetMetadataResponse(entity_value)

    def listMetadata(self, request):
        """
        Metadata will be directly retrieved from the remote backend.
        """
        self.logger.info(f"listMetadata: request={request}.")
        key_values = self.logging_client.list_metadata(
            request.partner_id,
            request.entity_key_start,
            request.entity_key_end,
            request.result_limit,
        )
        return ListMetadataResponse(key_values)


def main() -> None:
    logger = global_logger
    Utils.configure_logger(logger, "log/server.log")

    queue_manager = MemoryQueueManager()
    logging_client = MetaLoggingClient()
    metadata_manager = MetadataManager(queue_manager, logging_client)
    handler = LoggingServiceHandler(metadata_manager, logging_client)
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
