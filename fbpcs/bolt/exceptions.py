# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


class StageFailedException(ValueError):
    pass


class StageTimeoutException(RuntimeError):
    def __init__(
        self,
        msg: str,
        stage_cancelled: bool = False,
    ) -> None:
        super().__init__(msg)
        self.stage_cancelled = stage_cancelled


class WaitValidStatusTimeout(RuntimeError):
    pass


class NoServerIpsException(ValueError):
    pass


class IncompatibleStageError(RuntimeError):
    pass
