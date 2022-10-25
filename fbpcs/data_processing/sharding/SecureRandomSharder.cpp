/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/SecureRandomSharder.h"

namespace data_processing::sharder {

std::size_t SecureRandomSharder::getShardFor(
    const std::string& /* unused */,
    std::size_t /* unused */) {
  auto randomBytes = prg_->getRandomBytes(sizeof(uint32_t) + sizeof(__m128i));
  auto position = fbpcf::engine::util::mod(randomBytes, numShards_, ctx_);
  return position;
}

} // namespace data_processing::sharder
