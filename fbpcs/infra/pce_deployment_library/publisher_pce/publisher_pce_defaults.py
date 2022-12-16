# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
from enum import Enum

DIR = os.path.dirname
PCE_TERRAFORM_FILES = "pce/aws_terraform_template/common/pce"


class TerraformDefaults(str, Enum):
    PCE_TERRAFORM_FILE_LOCATION = os.path.join(
        DIR(DIR(DIR(os.path.realpath(__file__)))), PCE_TERRAFORM_FILES
    )
