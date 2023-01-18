#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class PrivateKeyReference:
    """The reference to a Private Key"""

    resource_id: str
    """The identifier used to access the private key"""

    region: str
    """The region where the private key can be accessed"""

    install_path: str
    """The path where the key should be installed"""


class PrivateKeyReferenceProvider(ABC):
    @abstractmethod
    def get_key_ref(self) -> Optional[PrivateKeyReference]:
        """Returns a private key reference"""
        pass


class NullPrivateKeyReferenceProvider(PrivateKeyReferenceProvider):
    """A null-pattern private key reference provider"""

    def get_key_ref(self) -> Optional[PrivateKeyReference]:
        """Returns a private key reference"""
        return None


class StaticPrivateKeyReferenceProvider(PrivateKeyReferenceProvider):
    """A private key reference provider that returns a static reference"""

    def __init__(self, resource_id: str, region: str, install_path: str) -> None:
        if not resource_id:
            raise ValueError("Must provide a `resource_id`")

        if not region:
            raise ValueError("Must provide a `region`")

        if not install_path:
            raise ValueError("Must provide an `install_path`")

        self.reference = PrivateKeyReference(resource_id, region, install_path)

    def get_key_ref(self) -> Optional[PrivateKeyReference]:
        """Returns a private key reference"""
        return self.reference
