# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class ProductConfig:
    """Stores metadata of product config in a private computation instance"""


@dataclass_json
@dataclass
class AttributionConfig(ProductConfig):
    """Stores metadata of attribution config in product config in a private computation instance

    Public attributes:

    Private attributes:

    """


@dataclass_json
@dataclass
class LiftConfig(ProductConfig):
    """Stores metadata of lift config in product config in a private computation instance

    Public attributes:

    Private attributes:

    """
