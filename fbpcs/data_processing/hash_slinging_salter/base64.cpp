/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "base64.h"
#include <folly/Format.h>
#include <openssl/evp.h>
#include <stdexcept>

namespace private_lift::base64 {

// adapted from https://stackoverflow.com/a/60580965
std::string encode(const std::string& input) {
  auto cStrInput = input.c_str();
  auto len = input.length();
  std::size_t numExpectedEncodedBytes = 4 * ((len + 2) / 3);
  auto output = reinterpret_cast<char*>(calloc(
      numExpectedEncodedBytes + 1,
      1)); //+1 for the terminating null that EVP_EncodeBlock adds on
  const auto numEncodedBytes = EVP_EncodeBlock(
      reinterpret_cast<unsigned char*>(output),
      reinterpret_cast<const unsigned char*>(cStrInput),
      len);
  if (numExpectedEncodedBytes != numEncodedBytes) {
    throw std::runtime_error(folly::sformat(
        "Expected {} encoded bytes, actually encoded {} bytes.",
        numExpectedEncodedBytes,
        numEncodedBytes));
  }
  return std::string{output};
}

// adapted from https://stackoverflow.com/a/60580965
std::string decode(const std::string& input) {
  auto cStrInput = input.c_str();
  auto len = input.length();
  std::size_t numExpectedDecodedBytes = 3 * len / 4;
  auto output =
      reinterpret_cast<unsigned char*>(calloc(numExpectedDecodedBytes + 1, 1));
  const auto numDecodedBytes = EVP_DecodeBlock(
      output, reinterpret_cast<const unsigned char*>(cStrInput), len);
  if (numExpectedDecodedBytes != numDecodedBytes) {
    throw std::runtime_error(folly::sformat(
        "Expected {} decoded bytes, actually decoded {} bytes.",
        numExpectedDecodedBytes,
        numDecodedBytes));
  }
  return std::string{reinterpret_cast<char*>(output)};
}

} // namespace private_lift::base64
