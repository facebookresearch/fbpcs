# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


class PCInfraConfigError(Exception):
    pass


class CannotFindDependencyError(PCInfraConfigError):
    """Raised when an invalid dependency is provided in mini config.yml"""

    def __init__(self, dep_key: str) -> None:
        msg = f"Unknown dependency {dep_key}, please provide a valid dependency in your config.yml."
        super().__init__(msg)
