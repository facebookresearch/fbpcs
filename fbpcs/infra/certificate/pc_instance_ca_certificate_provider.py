#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

from fbpcs.infra.certificate.certificate_provider import CertificateProvider
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)


class PCInstanceCaCertificateProvider(CertificateProvider):
    """
    A certificate provider which returns ca certificate value
    from PC instance repo.
    """

    def __init__(self, pc_instance: PrivateComputationInstance) -> None:
        self.pc_instance = pc_instance

    def get_certificate(self) -> Optional[str]:
        # TODO: implement this by retrieving ca certificate
        # from pc instance repo.
        raise NotImplementedError
