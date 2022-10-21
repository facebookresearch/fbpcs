#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import ABC, abstractmethod
from typing import Optional


class CertificateProvider(ABC):
    @abstractmethod
    def get_certificate(self) -> Optional[str]:
        pass
