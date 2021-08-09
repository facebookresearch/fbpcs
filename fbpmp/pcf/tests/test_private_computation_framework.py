#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import shutil

from fbpmp.pcf.games import ConversionLift
from fbpmp.pcf.mpc.tests.utils import MPCTestCase
from fbpmp.pcf.private_computation_framework import PrivateComputationFramework
from fbpmp.pcf.structs import Role, Status
from fbpmp.pcf.tests.async_utils import wait
from fbpmp.pcf.tests.utils import DummyGame, DummyMPCFramework, DummyPlayer


TEST_RUN_TIMEOUT = 5678
TEST_SLEEP_SECONDS = 0


class TestPrivateComputationFramework(MPCTestCase):
    def setUp(self):
        os.environ["RUN_TIMEOUT"] = str(TEST_RUN_TIMEOUT)

        self.game = DummyGame
        self.player = DummyPlayer.build(Role.PUBLISHER)
        self.other_players = [DummyPlayer.build(Role.PARTNER)]

        num_files = 2
        _, self.input_files = zip(
            *[
                self._make_input_csv(
                    game=ConversionLift, role=Role.PUBLISHER, num_records=10
                )
                for i in range(num_files)
            ]
        )
        self.output_files = self.input_files
        self.tempdirs = [f.parent for f in self.input_files]

        self.pcf = PrivateComputationFramework(
            game=self.game,
            input_files=self.input_files,
            output_files=self.output_files,
            player=self.player,
            other_players=self.other_players,
            mpc_cls=DummyMPCFramework,
            partner_sleep_seconds=TEST_SLEEP_SECONDS,
        )

        self.pcf_partner = PrivateComputationFramework(
            game=self.game,
            input_files=self.input_files,
            output_files=self.output_files,
            player=DummyPlayer.build(Role.PARTNER),
            other_players=[DummyPlayer.build(Role.PUBLISHER)],
            mpc_cls=DummyMPCFramework,
            partner_sleep_seconds=TEST_SLEEP_SECONDS,
        )

    def tearDown(self):
        for tempdir in self.tempdirs:
            shutil.rmtree(tempdir)

    def test_gen_frameworks(self):
        for i, fw in enumerate(self.pcf.mpc_frameworks):
            self.assertTrue(isinstance(fw, DummyMPCFramework))
            self.assertEqual(self.game, fw.game)
            self.assertEqual(self.input_files[i], fw.input_file)
            self.assertEqual(self.player, fw.player)
            self.assertEqual(self.other_players, fw.other_players)
            self.assertEqual(TEST_RUN_TIMEOUT, fw.run_timeout)

    def test_prepare_input(self):
        for status in Status:
            for fw in self.pcf.mpc_frameworks:
                fw.build(prepare_input=status)
            self.assertEqual(status, wait(self.pcf.prepare_input()))

    def test_run_mpc(self):
        expected_1 = {"key1": 1.0, "key2": 2.5, "key3": 99.9}
        expected_2 = {"key1": 9.0, "key2": 10.5, "key3": 199.9}
        self.assertEqual(2, len(self.pcf.mpc_frameworks))
        self.pcf.mpc_frameworks[0].build(run_mpc=expected_1.copy())
        self.pcf.mpc_frameworks[1].build(run_mpc=expected_2.copy())
        self.assertEqual(expected_1, wait(self.pcf.run_mpc())[0])
        self.assertEqual(expected_2, wait(self.pcf.run_mpc())[1])
        # Test on partner player too because it has a different logic in run_mpc
        self.assertEqual(2, len(self.pcf_partner.mpc_frameworks))
        self.pcf_partner.mpc_frameworks[0].build(run_mpc=expected_1.copy())
        self.pcf_partner.mpc_frameworks[1].build(run_mpc=expected_2.copy())
        self.assertEqual(expected_1, wait(self.pcf_partner.run_mpc())[0])
        self.assertEqual(expected_2, wait(self.pcf_partner.run_mpc())[1])
