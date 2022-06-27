# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from fbpcs.infra.pce_deployment_library.deploy_library.deploy_base.deploy_base import (
    DeployBase,
)


class Terraform(DeployBase):
    def create(self) -> None:
        pass

    def destroy(self) -> None:
        pass

    def init(self) -> None:
        pass

    def plan(self) -> None:
        pass

    def command(self) -> None:
        pass
