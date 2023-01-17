#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest

from fbpcs.infra.certificate.private_key import (
    NullPrivateKeyReferenceProvider,
    StaticPrivateKeyReferenceProvider,
)


class TestCertificateProviders(unittest.TestCase):
    def test_static_key_provider_missing_reference_id(self) -> None:
        # Arrange

        # Act & Assert
        with self.assertRaises(ValueError):
            StaticPrivateKeyReferenceProvider(
                resource_id="", region="region", install_path="a/path"
            )

    def test_static_key_provider_missing_region(self) -> None:
        # Arrange

        # Act & Assert
        with self.assertRaises(ValueError):
            StaticPrivateKeyReferenceProvider(
                resource_id="reference_id", region="", install_path="a/path"
            )

    def test_static_key_provider_missing_install_path(self) -> None:
        # Arrange

        # Act & Assert
        with self.assertRaises(ValueError):
            StaticPrivateKeyReferenceProvider(
                resource_id="reference_id", region="region", install_path=""
            )

    def test_static_key_provider(self) -> None:
        # Arrange
        expected_resource_id = "123456"
        expected_region = "region"
        expected_install_path = "a/path"
        provider = StaticPrivateKeyReferenceProvider(
            resource_id=expected_resource_id,
            region=expected_region,
            install_path=expected_install_path,
        )

        # Act
        key_ref = provider.get_key_ref()

        # Assert
        self.assertIsNotNone(key_ref)
        self.assertEqual(expected_resource_id, key_ref.resource_id)
        self.assertEqual(expected_region, key_ref.region)
        self.assertEqual(expected_install_path, key_ref.install_path)

    def test_null_provider(self) -> None:
        # Arrange

        # Act
        provider = NullPrivateKeyReferenceProvider()

        # Assert
        self.assertIsNone(provider.get_key_ref())
