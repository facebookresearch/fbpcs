# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
from typing import Dict

from fbpcs.infra.logging_service.server.common.data_model import MetadataEntity
from fbpcs.infra.logging_service.server.common.logging_client import LoggingClient


# Client to connect to Meta backend (Graph API)
class MetaLoggingClient(LoggingClient):
    def __init__(
        self,
    ) -> None:
        self.logger: logging.Logger = logging.getLogger()

    def put_metadata(
        self,
        entity: MetadataEntity,
    ) -> None:
        self.logger.info(f"put_metadata: entity={entity}.")

    def get_metadata(
        self,
        partner_id: str,
        entity_key: str,
    ) -> str:
        self.logger.info(f"get_metadata: entity_key={entity_key}.")
        # returning fake data
        return "value1"

    def list_metadata(
        self,
        partner_id: str,
        entity_key_start: str,
        entity_key_end: str,
        result_limit: int,
    ) -> Dict[str, str]:
        self.logger.info(
            f"list_metadata: entity_key_start={entity_key_start}, entity_key_end={entity_key_end}."
        )
        # returning fake data
        key_values = {}
        key_values["key-a"] = "val-a"
        return key_values
