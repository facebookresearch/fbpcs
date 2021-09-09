#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from fbpcs.pcf.errors import MPCStartupError
from fbpcs.pcf.tests.utils import Dummy2PCFramework, DummyGame, DummyPlayer


TEST_CONNECT_TIMEOUT = 789
TEST_RUN_TIMEOUT = 456


class Test2PCFramework(unittest.TestCase):
    def test_pre_setup(self):
        # Initiate with 2 total players and no exception should be thrown
        Dummy2PCFramework(
            game=DummyGame,
            input_file="",
            output_file="",
            player=DummyPlayer,
            other_players=[DummyPlayer],  # 2 total player
            connect_timeout=TEST_CONNECT_TIMEOUT,
            run_timeout=TEST_RUN_TIMEOUT,
        )

        # Initiate with 3 total players and MPCStartupError should be thrown
        with self.assertRaises(MPCStartupError):
            Dummy2PCFramework(
                game=DummyGame,
                input_file="",
                output_file="",
                player=DummyPlayer,
                other_players=[DummyPlayer, DummyPlayer],  # 3 total player
                connect_timeout=TEST_CONNECT_TIMEOUT,
                run_timeout=TEST_RUN_TIMEOUT,
            )
