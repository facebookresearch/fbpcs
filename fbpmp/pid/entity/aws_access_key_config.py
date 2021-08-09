#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import abc
from typing import Dict


class AWSAccessKeyConfig(abc.ABC):
    @abc.abstractmethod
    def get_creds(self) -> Dict[str, str]:
        pass
