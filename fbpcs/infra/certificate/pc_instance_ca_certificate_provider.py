#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

from fbpcs.infra.certificate.certificate_provider import CertificateProvider

# pyre-fixme[21]: Could not find module
#  `fbpcs.private_computation.entity.private_computation_instance`.
from fbpcs.private_computation.entity.private_computation_instance import (
    PrivateComputationInstance,
)


class PCInstanceCaCertificateProvider(CertificateProvider):
    """
    A certificate provider which returns ca certificate value
    from PC instance repo.
    """

    # pyre-fixme[11]: Annotation `PrivateComputationInstance` is not defined as a type.
    def __init__(self, pc_instance: PrivateComputationInstance) -> None:
        # pyre-fixme[4]: Attribute must be annotated.
        self.pc_instance = pc_instance

    def get_certificate(self) -> Optional[str]:
        return self.pc_instance.infra_config.ca_certificate
