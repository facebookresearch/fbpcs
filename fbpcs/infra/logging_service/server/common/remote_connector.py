# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import abc
from typing import Dict

from common.data_model import MetadataEntity


# Connector interface for putting and getting log metadata on remote backend.
class RemoteConnector(abc.ABC):
    @abc.abstractmethod
    def put_metadata(
        self,
        entity: MetadataEntity,
    ) -> None:
        pass

    @abc.abstractmethod
    def get_metadata(
        self,
        partner_id: str,
        entity_key: str,
    ) -> str:
        pass

    @abc.abstractmethod
    def list_metadata(
        self,
        partner_id: str,
        entity_key_start: str,
        entity_key_end: str,
        result_limit: int,
    ) -> Dict[str, str]:
        pass
