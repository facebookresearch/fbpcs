#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.infra.certificate.certificate_provider import CertificateProvider


class NullCertificateProvider(CertificateProvider):
    """
    A null pattern for the abstract class CertificateProvider
    """

    def get_certificate(self) -> None:
        return None
