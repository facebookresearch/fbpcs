#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import pathlib
import random
import shutil
import tempfile
import unittest

from fbpcs.pcf.structs import Role, Status
from fbpcs.pcf.tests.async_utils import wait
from fbpcs.pcf.tests.utils import DummyGame, DummyMPCFramework, DummyPlayer


TEST_CONNECT_TIMEOUT = 0
TEST_RUN_TIMEOUT = 456


class TestMPCBase(unittest.TestCase):
    def setUp(self):
        csv_id = random.randint(0, 2 ** 32)
        self.tempdir = tempfile.mkdtemp()
        self.input_file = pathlib.Path(self.tempdir) / f"{csv_id}.csv"
        self.output_file = self.input_file
        self.mpc_framework = DummyMPCFramework(
            game=DummyGame,
            input_file=self.input_file,
            output_file=self.output_file,
            player=DummyPlayer.build(Role.PUBLISHER),
            other_players=[DummyPlayer.build(Role.PARTNER)],
            connect_timeout=TEST_CONNECT_TIMEOUT,
            run_timeout=TEST_RUN_TIMEOUT,
        )

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_supports_game(self):
        for support in [True, False]:
            self.mpc_framework.build(supports_game=support)
            self.assertEqual(support, self.mpc_framework.supports_game(DummyGame))

    def test_prepare_input(self):
        for status in Status:
            self.mpc_framework.build(prepare_input=status)
            self.assertEqual(status, wait(self.mpc_framework.prepare_input()))

    def test_run_mpc(self):
        expected = {"key1": 100.0, "key2": 0.5, "key3": 99.9}
        self.mpc_framework.build(run_mpc=expected)
        self.assertEqual(expected, wait(self.mpc_framework.run_mpc()))
