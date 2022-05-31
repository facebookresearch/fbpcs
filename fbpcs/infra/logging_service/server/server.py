# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Logging Service server


Usage:
    logging_service_server [options]

Options:
    --ipv6                  Server socket listens at IPv6/INET6 family instead of IPv4/INET family
    --port=<port>           Port number to listen at. [default: 9090]
    -h --help               Show this help
"""

import logging
import os
import socket
from types import ModuleType

import schema
import thriftpy2
from docopt import docopt
from fbpcs.infra.logging_service.server.common.logging_client import LoggingClient
from fbpcs.infra.logging_service.server.common.memory_queue_manager import (
    MemoryQueueManager,
)
from fbpcs.infra.logging_service.server.common.meta_logging_client import (
    MetaLoggingClient,
)
from fbpcs.infra.logging_service.server.common.metadata_manager import MetadataManager
from fbpcs.infra.logging_service.server.common.utils import Utils
from thriftpy2.protocol import TBinaryProtocolFactory
from thriftpy2.server import TThreadedServer
from thriftpy2.thrift import TProcessor
from thriftpy2.transport import TBufferedTransportFactory, TServerSocket


# Socket timeout from client, in millisecond
CLIENT_TIMEOUT_MS = 10000

# Handler for the logging service API's
class LoggingServiceHandler:
    def __init__(
        self,
        metadata_manager: MetadataManager,
        logging_client: LoggingClient,
        logging_service_thrift: ModuleType,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.metadata_manager = metadata_manager
        self.logging_client = logging_client
        self.logging_service_thrift = logging_service_thrift

    # pyre-ignore
    def putMetadata(self, request) -> object:
        """
        Each metadata entity will be queued in the metadata manager, and
        then uploaded to remote backend.
        """
        self.logger.info(f"putMetadata: request={request}.")
        self.metadata_manager.put_metadata(
            request.partner_id, request.entity_key, request.entity_value
        )
        res = self.logging_service_thrift.PutMetadataResponse()
        self.logger.info(f"PutMetadataResponse: {res}")
        return res

    # pyre-ignore
    def getMetadata(self, request) -> object:
        """
        Metadata will be directly retrieved from the remote backend.
        """
        self.logger.info(f"getMetadata: request={request}.")
        entity_value = self.logging_client.get_metadata(
            request.partner_id, request.entity_key
        )
        res = self.logging_service_thrift.GetMetadataResponse(entity_value)
        self.logger.info(f"GetMetadataResponse: {res}")
        return res

    # pyre-ignore
    def listMetadata(self, request) -> object:
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
        res = self.logging_service_thrift.ListMetadataResponse(key_values)
        self.logger.info(f"ListMetadataResponse: {res}")
        return res


def main() -> None:
    Utils.configure_logger("log/server.log")
    logger = logging.getLogger(__name__)

    s = schema.Schema(
        {
            "--ipv6": bool,
            "--port": schema.Use(int),
            "--help": bool,
        }
    )

    arguments = s.validate(docopt(__doc__))

    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    thrift_path = os.path.join(current_script_dir, "thrift/logging_service.thrift")
    logger.info(f"Loading the thrift definition from: {thrift_path};")
    logging_service_thrift = thriftpy2.load(
        thrift_path,
        module_name="logging_service_thrift",
    )

    if arguments["--ipv6"]:
        socket_family = socket.AF_INET6
        any_host_interface = "::"
        socket_family_name = "IPv6"
    else:
        socket_family = socket.AF_INET
        any_host_interface = "0.0.0.0"
        socket_family_name = "IPv4"

    server_port = arguments["--port"]

    queue_manager = MemoryQueueManager()
    logging_client = MetaLoggingClient()
    metadata_manager = MetadataManager(queue_manager, logging_client)
    handler = LoggingServiceHandler(
        metadata_manager, logging_client, logging_service_thrift
    )

    proc = TProcessor(logging_service_thrift.LoggingService, handler)
    server = TThreadedServer(
        proc,
        TServerSocket(
            host=any_host_interface,
            socket_family=socket_family,
            port=server_port,
            client_timeout=CLIENT_TIMEOUT_MS,
        ),
        iprot_factory=TBinaryProtocolFactory(),
        itrans_factory=TBufferedTransportFactory(),
    )

    logger.info(
        f"Logging service server listens host={any_host_interface}[{socket_family_name}], port={server_port}."
    )
    server.serve()
    logger.info("done.")


if __name__ == "__main__":
    main()
