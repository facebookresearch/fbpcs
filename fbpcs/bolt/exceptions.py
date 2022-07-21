# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


class StageFailedException(ValueError):
    pass


class StageTimeoutException(RuntimeError):
    pass


class WaitValidStatusTimeout(RuntimeError):
    pass


class NoServerIpsException(ValueError):
    pass


class IncompatibleStageError(RuntimeError):
    pass
