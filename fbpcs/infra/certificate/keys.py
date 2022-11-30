#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)


class EllipticCurvePublicKey:
    """An elliptic curve public key"""

    def __init__(self, key: ec.EllipticCurvePublicKeyWithSerialization) -> None:
        self._public_key = key

    def verify(self, signature: bytes, data: bytes) -> None:
        """Verifies one block of data was signed with the private key associated with this public key"""

        hash_algorithm = hashes.SHA256()  # most common, secure hash for ECDSA
        signature_algorithm = ec.ECDSA(hash_algorithm)  # only one currently supported

        return self._public_key.verify(signature, data, signature_algorithm)

    def public_bytes(self) -> bytes:
        """Serializes the key to bytes of an unencrypted PEM in Subject Public Key Info format"""

        encoding = Encoding.PEM  # most commonly supported
        key_format = PublicFormat.SubjectPublicKeyInfo  # standard

        return self._public_key.public_bytes(encoding, key_format)


class EllipticCurvePrivateKey:
    """An elliptic curve private key"""

    def __init__(self, key: ec.EllipticCurvePrivateKeyWithSerialization) -> None:
        self._key = key

    def sign(self, data: bytes) -> bytes:
        """Signs one block of data, which can be verified using the paired public key"""

        hash_algorithm = hashes.SHA256()  # most common, secure hash for ECDSA
        signature_algorithm = ec.ECDSA(hash_algorithm)  # only one currently supported

        return self._key.sign(data, signature_algorithm)

    def private_bytes(self) -> bytes:
        """Serializes the key to bytes of an unencrypted PEM in PKCS8 format"""

        encoding = Encoding.PEM  # most commonly supported
        key_format = PrivateFormat.PKCS8  # modern, well-supported
        encryption = NoEncryption()  # need raw bytes, may support encryption later

        return self._key.private_bytes(encoding, key_format, encryption)

    def public_key(self) -> EllipticCurvePublicKey:
        return EllipticCurvePublicKey(self._key.public_key())


class KeyFactory:
    """A factory which produces cryptographic keys"""

    @staticmethod
    def create_ec_key() -> EllipticCurvePrivateKey:
        curve = (
            ec.SECP256R1
        )  # only support NIST P-256 key for now, most commonly used ECC for certificates

        return EllipticCurvePrivateKey(ec.generate_private_key(curve))
