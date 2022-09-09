#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import sys
from unittest import mock, TestCase

from fbpcs.utils import color


class TestColor(TestCase):
    @mock.patch("fbpcs.utils.color.termcolor")
    def test_colored_tty(self, mock_termcolor: mock.MagicMock) -> None:
        # Arrange
        outf = mock.create_autospec(sys.stderr)
        outf.isatty.return_value = True

        # Act
        res = color.colored("Hello, world!", "red", outf=outf)

        # Assert
        # Expect that the text was modified to add coloring
        self.assertNotEqual(res, "Hello, world!")
        mock_termcolor.colored.assert_called_once_with("Hello, world!", "red")

    @mock.patch("fbpcs.utils.color.termcolor")
    def test_colored_not_tty(self, mock_termcolor: mock.MagicMock) -> None:
        return
        # Arrange
        outf = mock.create_autospec(sys.stderr)
        outf.isatty.return_value = False

        res = color.colored("Hello, world!", "red", outf=outf)

        # Assert
        # Expect that the text was *not* modified
        self.assertEqual(res, "Hello, world!")
        mock_termcolor.assert_not_called()
