/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <climits>
#include <functional>
#include <random>
#include <stdexcept>

#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include <folly/ssl/OpenSSLPtrTypes.h>

#include "fbpcs/emp_games/common/Crypto.h"

namespace private_measurement::crypto {

folly::ssl::EvpPkeyUniquePtr getRandomRSAKeyPair(size_t length) {
  // Generate RSA keypair
  EVP_PKEY* pkey = nullptr;
  folly::ssl::EvpPkeyCtxUniquePtr ctx(
      EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr));
  checkSuccessOrThrow(
      EVP_PKEY_keygen_init(ctx.get()), "EVP_PKEY_kegen_init failed.");
  checkSuccessOrThrow(
      EVP_PKEY_CTX_set_rsa_keygen_bits(ctx.get(), length),
      "EVP_PKEY_CTX_set_rsa_keygen_bits failed.");
  checkSuccessOrThrow(
      EVP_PKEY_keygen(ctx.get(), &pkey), "EVP_PKEY_keygen failed.");
  return folly::ssl::EvpPkeyUniquePtr(pkey);
}

void encryptionTestHelper(
    const std::vector<unsigned char>& plaintext,
    folly::ssl::EvpPkeySharedPtr keyPair1,
    folly::ssl::EvpPkeySharedPtr keyPair2) {
  HybridCipher cipher;
  auto cipherMsg = cipher.encrypt(plaintext, keyPair1);

  // Decrypt with the correct private key
  auto decryptedText = cipher.decrypt(cipherMsg, keyPair1);
  EXPECT_THAT(plaintext, testing::ContainerEq(decryptedText));

  // Decrypt with a wrong private key
  EXPECT_THROW(cipher.decrypt(cipherMsg, keyPair2), OpenSSLException);
}

using randomBytesEngine = std::independent_bits_engine<
    std::default_random_engine,
    CHAR_BIT,
    unsigned char>;
inline std::vector<unsigned char> generateRandomBytes(size_t size) {
  randomBytesEngine rbe;
  std::vector<unsigned char> res(size);
  std::generate(res.begin(), res.end(), std::ref(rbe));
  return res;
}

TEST(CryptoUtilTest, testEncryptionDecryption) {
  const size_t keyLength = 1024;
  folly::ssl::EvpPkeySharedPtr keyPair1 = getRandomRSAKeyPair(keyLength);
  folly::ssl::EvpPkeySharedPtr keyPair2 = getRandomRSAKeyPair(keyLength);

  // Simple string
  const std::string inputMsg =
      "Hello world \4\5 I'm \t \17\22 test \177 string";
  auto inputBytes =
      std::vector<unsigned char>(inputMsg.cbegin(), inputMsg.cend());
  inputBytes[6] = '\0';
  encryptionTestHelper(inputBytes, keyPair1, keyPair2);

  // Empty string
  encryptionTestHelper(std::vector<unsigned char>(), keyPair1, keyPair2);

  // All zero bytes
  encryptionTestHelper(std::vector<unsigned char>(100, 0), keyPair1, keyPair2);

  // Random bytes
  encryptionTestHelper(generateRandomBytes(2000), keyPair1, keyPair2);
}

} // namespace private_measurement::crypto
