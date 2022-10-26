#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

from fbpcs.infra.certificate.certificate_provider import CertificateProvider


class BasicCaCertificateProvider(CertificateProvider):
    """
    A certificate provider which simply returns certificate
    based on the argument upon initialization.
    """

    def __init__(self, ca_certificate: str) -> None:
        self.ca_certificate = ca_certificate

    def get_certificate(self) -> Optional[str]:
        return self.ca_certificate
