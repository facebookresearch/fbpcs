#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

import botocore

# pyre-fixme[21]: Could not find module `fbpcp.intern.gateway.aws_fb`.
from fbpcp.intern.gateway.aws_fb import FBAWSGateway
from fbpcs.service.workflow_sfn import SfnWorkflowService


# pyre-fixme[11]: Annotation `FBAWSGateway` is not defined as a type.
class FBSfnWorkflowService(FBAWSGateway, SfnWorkflowService):
    def __init__(
        self,
        region: str,
        account: str,
        role: Optional[str] = None,
    ) -> None:
        # pyre-fixme[28]: Unexpected keyword argument `account`.
        super().__init__(account=account, role=role, region=region)
        # pyre-fixme[16]: `FBSfnWorkflowService` has no attribute `session_gen`.
        self.client: botocore.client.BaseClient = self.session_gen.get_client(
            "stepfunctions"
        )
