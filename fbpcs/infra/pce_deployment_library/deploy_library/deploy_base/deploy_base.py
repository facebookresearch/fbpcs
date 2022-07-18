# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import ABC, abstractmethod

from fbpcs.infra.pce_deployment_library.deploy_library.models import RunCommandResult


class DeployBase(ABC):
    @abstractmethod
    def create(self) -> RunCommandResult:
        pass

    @abstractmethod
    def destroy(self) -> RunCommandResult:
        pass

    @abstractmethod
    def plan(self) -> None:
        pass

    @abstractmethod
    def run_command(self) -> RunCommandResult:
        pass
