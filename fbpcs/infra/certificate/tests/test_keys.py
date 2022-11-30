#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
)

from fbpcs.infra.certificate.keys import EllipticCurvePrivateKey, KeyFactory


class TestKeys(unittest.TestCase):
    def test_key_factory_create_ec_key(self) -> None:
        # Act
        key = KeyFactory.create_ec_key()

        # Assert
        self.assertIsNotNone(key)
        self.assertIsInstance(key, EllipticCurvePrivateKey)

    def test_ec_private_key_serialization(self) -> None:
        # Arrange
        key = KeyFactory.create_ec_key()
        expected_curve = ec.SECP256R1()
        expected_key_length = 256

        # Act
        serialized_key = key.private_bytes()
        deserialized_key = load_pem_private_key(serialized_key, password=None)

        # Assert
        self.assertIsInstance(
            deserialized_key, ec.EllipticCurvePrivateKey
        )  # implicitly verifies that key was serialized to unencrypted PEM

        curve = deserialized_key.curve
        self.assertEqual(curve.name, expected_curve.name)
        self.assertEqual(curve.key_size, expected_curve.key_size)

        self.assertEqual(deserialized_key.key_size, expected_key_length)

    def test_ec_public_key_serialization(self) -> None:
        # Arrange
        key = KeyFactory.create_ec_key()
        expected_curve = ec.SECP256R1()
        expected_key_length = 256

        # Act
        serialized_key = key.public_key().public_bytes()
        deserialized_key = load_pem_public_key(serialized_key)

        # Assert
        self.assertIsInstance(
            deserialized_key, ec.EllipticCurvePublicKey
        )  # implicitly verifies that key was serialized to PEM

        curve = deserialized_key.curve
        self.assertEqual(curve.name, expected_curve.name)
        self.assertEqual(curve.key_size, expected_curve.key_size)

        self.assertEqual(deserialized_key.key_size, expected_key_length)

    def test_ec_key_signature_verification_failure(self) -> None:
        # Arrange
        key = KeyFactory.create_ec_key()
        actual_data = "some test information".encode("utf-8")
        non_matching_data = "invalid data".encode("utf-8")

        # Act
        signature = key.sign(actual_data)

        # Assert
        with self.assertRaises(InvalidSignature):
            key.public_key().verify(signature, non_matching_data)

    def test_ec_key_signature_verification_success(self) -> None:
        # Arrange
        key = KeyFactory.create_ec_key()
        actual_data = "some test information".encode("utf-8")

        # Act
        signature = key.sign(actual_data)

        # Assert
        key.public_key().verify(signature, actual_data)  # valid if no error
