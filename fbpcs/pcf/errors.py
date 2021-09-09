#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


class MPCStartupError(RuntimeError):
    """
    Represents an error that occurred during MPC startup.
    This could be due to something like unexpected data format in prepare_input.
    """


class MPCRuntimeError(RuntimeError):
    """
    Represents a generic error that occurred while running an MPC program.
    For more detailed results, the appropriate logs would have more info.
    """

    def __init__(self, mpc_exit_code):
        self.message = f"The MPC program failed with exit code {mpc_exit_code}"


class SetupAlreadyDoneError(RuntimeError):
    """
    Error raised when pre_setup is called more than once for an MPCFramework.
    Since this can potentially cause some inconsistent or undefined behavior for
    a given framework, we throw this Exception to alert the programmer that
    something odd has happened.
    """

    def __init__(self):
        self.message = (
            "Setup was already called once. This should not happen more than once."
        )


class UnsupportedGameForFrameworkError(NotImplementedError):
    """
    This class represents the error thrown when an MPCFramework is attempting
    to be instantiated with an unsupported game. Realistically, this should
    never occur in a production environment since our default MPCFramework
    should support all games that could be run by PCF.
    """

    def __init__(self, framework, game):
        self.message = (
            f"The game {game.name} is not supported"
            f" by {framework.__class__.__name__} at this time"
        )
