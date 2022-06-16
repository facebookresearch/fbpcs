# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import unittest

from fbpcs.infra.pce_deployment_library.cloud_library.cloud_factory import CloudFactory
from fbpcs.infra.pce_deployment_library.cloud_library.defaults import CloudPlatforms


class TestCloudFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.test_cloud_factory = CloudFactory()

    def test_supported_cloud_platforms(self) -> None:
        expected = CloudPlatforms.list()

        self.assertEqual(
            expected, self.test_cloud_factory.get_supported_cloud_platforms()
        )

    def test_create_cloud_object_aws(self) -> None:
        expected = CloudPlatforms.AWS

        cloud_object = self.test_cloud_factory.create_cloud_object(
            cloud_type=CloudPlatforms.AWS
        )
        self.assertEqual(expected, cloud_object.cloud_type())

    def test_create_cloud_object_gcp(self) -> None:
        expected = CloudPlatforms.GCP

        cloud_object = self.test_cloud_factory.create_cloud_object(
            cloud_type=CloudPlatforms.GCP
        )

        self.assertEqual(expected, cloud_object.cloud_type())
