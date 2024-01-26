# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from fbpcs.private_computation.entity.pcs_feature import PCSFeature


class TestPcsFeatureEnum(unittest.TestCase):
    def test_pcs_feature_enum(self) -> None:
        for test_pcs_feature in PCSFeature:
            with self.subTest(test_pcs_feature=test_pcs_feature):
                feature = PCSFeature.from_str(test_pcs_feature.value)

                # Test to make sure  from_str is case insensitive.
                uppercase_feature = PCSFeature.from_str(test_pcs_feature.value.upper())

                # pyre-fixme[16]: `TestPcsFeatureEnum` has no attribute `assertEquals`.
                self.assertEquals(feature, test_pcs_feature)
                self.assertEquals(uppercase_feature, test_pcs_feature)

    def test_pcs_feature_enum_by_name(self) -> None:
        feature = PCSFeature(PCSFeature.PCS_DUMMY.name)
        # pyre-fixme[16]: `TestPcsFeatureEnum` has no attribute `assertEquals`.
        self.assertEquals(feature, PCSFeature.PCS_DUMMY)

    def test_pcs_feature_enum_unkown_by_value(self) -> None:
        feature = PCSFeature("unknown_feature")
        # pyre-fixme[16]: `TestPcsFeatureEnum` has no attribute `assertEquals`.
        self.assertEquals(feature, PCSFeature.UNKNOWN)

    def test_pcs_feature_enum_casefold_by_value(self) -> None:
        feature = PCSFeature("PCS_Dummy_Feature")
        # pyre-fixme[16]: `TestPcsFeatureEnum` has no attribute `assertEquals`.
        self.assertEquals(feature, PCSFeature.PCS_DUMMY)

    def test_pcs_feature_enum_unkown(self) -> None:
        feature = PCSFeature.from_str("unknown_feature")
        # pyre-fixme[16]: `TestPcsFeatureEnum` has no attribute `assertEquals`.
        self.assertEquals(feature, PCSFeature.UNKNOWN)
