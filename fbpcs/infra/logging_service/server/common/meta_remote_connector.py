# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from typing import Dict

from common.data_model import MetadataEntity
from common.remote_connector import RemoteConnector


# Connector to connect to Meta backend
class MetaRemoteConnector(RemoteConnector):
    def __init__(
        self,
    ):
        self.logger = logging.getLogger()

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
        key_values = {}
        key_values["key-a"] = "val-a"
