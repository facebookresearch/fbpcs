/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fmt/format.h>
#include <folly/ssl/OpenSSLPtrTypes.h>
#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace private_measurement::crypto {

class OpenSSLException : public std::runtime_error {
 public:
  explicit OpenSSLException(const std::string& msg, int ret)
      : std::runtime_error(
            fmt::format("OpenSSL exception: {}. ret={}.", msg, ret)) {}
};

inline void checkSuccessOrThrow(int ret, std::string errMsg, int succRet = 1) {
  if (ret != succRet) {
    throw OpenSSLException(errMsg, ret);
  }
}

template <typename TFailureFn>
inline void
checkSuccessOrThrow(int ret, std::string errMsg, TFailureFn&& isFailure) {
  static_assert(std::is_invocable_r_v<bool, TFailureFn, int>, "Type mismatch!");
  if (isFailure(ret)) {
    throw OpenSSLException(errMsg, ret);
  }
}

// Wrapper for C-style OpenSSL hybrid encryption/decryption
class HybridCipher {
 public:
  const EVP_CIPHER* kSymmetricCipher = EVP_aes_256_cbc();

  struct CipherMessage {
    CipherMessage(
        size_t ciphertextLen,
        size_t ivLen,
        size_t sessionKeyLen,
        size_t sigLen)
        : ciphertext(ciphertextLen),
          iv(ivLen), // required by some symmetric ciphers
          sessionKey(sessionKeyLen), // used for encrypting the ciphertext
          signature(sigLen) {}

    std::vector<unsigned char> ciphertext;
    std::vector<unsigned char> iv; // required by some symmetric ciphers
    std::vector<unsigned char> sessionKey; // used for encrypting the ciphertext
    std::vector<unsigned char> signature;

    // TODO: Add serialization / deserialization methods
  };

  // TODO: Add message signing
  CipherMessage encrypt(
      const std::vector<unsigned char>& plaintext,
      folly::ssl::EvpPkeySharedPtr pubKey);

  // TODO: Add message signature verification
  std::vector<unsigned char> decrypt(
      const CipherMessage& cipherMsg,
      folly::ssl::EvpPkeySharedPtr privKey);
};

} // namespace private_measurement::crypto
