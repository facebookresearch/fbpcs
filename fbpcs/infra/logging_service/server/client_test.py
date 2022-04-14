# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging

from common.utils import Utils
from meta.private_computation import LoggingService
from meta.private_computation.ttypes import (
    GetMetadataRequest,
    ListMetadataRequest,
    PutMetadataRequest,
)
from thrift import Thrift
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport


global_logger = logging.getLogger()

# Client for ad-hoc testing
def main():
    logger = global_logger
    Utils.configure_logger(logger, "log/client_test.log")

    # send requests
    try:
        transport = TSocket.TSocket("localhost", Utils.get_server_port())
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = LoggingService.Client(protocol)
        transport.open()

        # put metadata might be processed with latency.
        request = PutMetadataRequest("partner1", "key2", "value2")
        response = client.putMetadata(request)
        logger.info(f"putMetadata: response: {response}.")

        request = GetMetadataRequest("partner1", "key1")
        response = client.getMetadata(request)
        logger.info(f"GetMetadataRequest: response: {response}.")

        request = ListMetadataRequest("partner1", "start1", "end1", 10)
        response = client.listMetadata(request)
        logger.info(f"ListMetadataRequest: response: {response}.")

        transport.close()
    except Thrift.TException as ex:
        logger.error(f"Exception received: {str(ex)}.")


if __name__ == "__main__":
    main()
