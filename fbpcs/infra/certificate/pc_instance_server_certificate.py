#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.infra.certificate.sample_tls_certificates import SAMPLE_SERVER_CERTIFICATE
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)


class PCInstanceServerCertificateProvider(CertificateProvider):
    """
    A certificate provider which returns server certificate value
    from PC instance repo.
    """

    def __init__(self, pc_instance: PrivateComputationInstance) -> None:
        self.pc_instance = pc_instance

    def get_certificate(self) -> Optional[str]:
        """
        Get certificate value from pc instance repo.
        """
        return self.pc_instance.infra_config.server_certificate
