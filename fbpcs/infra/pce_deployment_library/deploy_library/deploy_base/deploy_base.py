# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from abc import ABC, abstractmethod


class DeployBase(ABC):
    @abstractmethod
    def create(self) -> None:
        pass

    @abstractmethod
    def destroy(self) -> None:
        pass

    @abstractmethod
    def init(self) -> None:
        pass

    @abstractmethod
    def plan(self) -> None:
        pass

    @abstractmethod
    def command(self) -> None:
        pass
