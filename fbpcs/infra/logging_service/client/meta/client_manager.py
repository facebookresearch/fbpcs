# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import os
from types import ModuleType
from typing import Dict, Optional

import thriftpy2
from thriftpy2.rpc import make_client
from thriftpy2.thrift import TClient


class ClientManager:
    def __init__(
        self,
        server_host: str,
        server_port: int,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger(__name__)

        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        thrift_path = os.path.join(
            current_script_dir, "../../server/thrift/logging_service.thrift"
        )
        self.logger.info(f"Loading the thrift definition from: {thrift_path};")
        self.logging_service_thrift: ModuleType = thriftpy2.load(
            thrift_path,
            module_name="logging_service_thrift",
        )

        self.client: Optional[TClient] = None
        self._init_thrift_client(server_host, server_port)

    def put_metadata(
        self,
        partner_id: str,
        entity_key: str,
        entity_value: str,
    ) -> None:
        if not self.client:
            return
        request = self.logging_service_thrift.PutMetadataRequest(
            partner_id, entity_key, entity_value
        )
        # pyre-ignore
        response = self.client.putMetadata(request)
        self.logger.info(f"put_metadata: response: {response}.")

    def get_metadata(
        self,
        partner_id: str,
        entity_key: str,
    ) -> Optional[str]:
        if not self.client:
            return None
        request = self.logging_service_thrift.GetMetadataRequest(partner_id, entity_key)
        # pyre-ignore
        response = self.client.getMetadata(request)
        self.logger.info(f"get_metadata: response: {response}.")
        return response.entity_value

    def list_metadata(
        self,
        partner_id: str,
        entity_key_start: str,
        entity_key_end: str,
        result_limit: int,
    ) -> Dict[str, str]:
        if not self.client:
            return {}
        request = self.logging_service_thrift.ListMetadataRequest(
            partner_id, entity_key_start, entity_key_end, result_limit
        )
        # pyre-ignore
        response = self.client.listMetadata(request)
        self.logger.info(f"list_metadata: response: {response}.")
        return response.key_values

    def close(
        self,
    ) -> None:
        if self.client:
            self.client.close()

    def _init_thrift_client(
        self,
        server_host: str,
        server_port: int,
    ) -> None:
        if not server_host or not server_port:
            self.logger.warning(
                "client manager will become no-op due to missing server host and/or port."
            )
            return

        self.logger.info("Creating the client and connecting...")
        self.client = make_client(
            self.logging_service_thrift.LoggingService, server_host, server_port
        )
