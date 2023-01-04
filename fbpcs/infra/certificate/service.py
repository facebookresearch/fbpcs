#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import ABC, abstractmethod


class X509Certificate(ABC):
    """A class which represents a X.509 Certificate"""

    @abstractmethod
    def public_bytes(self) -> bytes:
        """Returns the certificate as PEM-encoded bytes"""
        pass
