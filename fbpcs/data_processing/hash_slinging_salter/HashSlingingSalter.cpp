/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "HashSlingingSalter.hpp"
#include "base64.h"

#include <openssl/hmac.h>
#include <array>

namespace private_lift::hash_slinging_salter {

// https://stackoverflow.com/a/64570079
std::string saltedHash(const std::string& id, const std::string& key) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> hash;
  unsigned int hashLen;

  HMAC(
      EVP_sha256(),
      key.data(),
      static_cast<int>(key.size()),
      reinterpret_cast<unsigned char const*>(id.data()),
      static_cast<int>(id.size()),
      hash.data(),
      &hashLen);

  return std::string{reinterpret_cast<char const*>(hash.data()), hashLen};
}

std::string base64SaltedHashFromBase64Key(
    const std::string& id,
    const std::string& base64Key) {
  return base64::encode(saltedHash(id, base64::decode(base64Key)));
}

} // namespace private_lift::hash_slinging_salter
