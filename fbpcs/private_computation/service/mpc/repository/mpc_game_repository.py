#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc

from fbpcs.private_computation.service.mpc.entity.mpc_game_config import MPCGameConfig


class MPCGameRepository(abc.ABC):
    @abc.abstractmethod
    def get_game(self, name: str) -> MPCGameConfig:
        pass
