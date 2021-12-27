/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/HashBasedSharder.h"

#include <arpa/inet.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <fbpcf/io/FileManagerUtil.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include "fbpcs/data_processing/common/FilepathHelpers.h"
#include "fbpcs/data_processing/common/Logging.h"
#include "fbpcs/data_processing/common/S3CopyFromLocalUtil.h"
#include "fbpcs/data_processing/hash_slinging_salter/HashSlingingSalter.hpp"

namespace data_processing::sharder {
namespace detail {
std::vector<uint8_t> toBytes(const std::string& key) {
  std::vector<uint8_t> res(key.begin(), key.end());
  return res;
}

int32_t bytesToInt(const std::vector<uint8_t>& bytes) {
  int32_t res = 0;

  auto bytesInSizeT = sizeof(res) / sizeof(uint8_t);
  auto end = bytes.begin() + std::min(bytesInSizeT, bytes.size());
  std::copy(bytes.begin(), end, reinterpret_cast<uint8_t*>(&res));

  // Because we could be in a bizarre scenario where the publisher machine's
  // endianness differs from the partner machine's endianness, we rearrange the
  // bytes now to ensure a consistent representation. We assume the previously
  // copied bytes are in "network byte order" and convert them to a host long.
  return ntohl(res);
}
} // namespace detail

std::size_t HashBasedSharder::getShardFor(
    const std::string& id,
    std::size_t numShards) {
  auto toInt = detail::bytesToInt(detail::toBytes(id));
  return toInt % numShards;
}

void HashBasedSharder::shardLine(
    std::string line,
    const std::vector<std::unique_ptr<std::ofstream>>& outFiles) {
  auto commaPos = line.find_first_of(",");
  auto id = line.substr(0, commaPos);
  auto numShards = outFiles.size();
  if (hmacKey_.empty()) {
    // Assumption: the string is *already* an HMAC hashed value
    // If hmacBase64Key is empty, the hashing already happened upstream.
    // This means we can reinterpret the id as a base64-encoded string.
    auto shard = getShardFor(id, numShards);
    *outFiles.at(shard) << line << "\n";
  } else {
    auto base64SaltedId =
        private_lift::hash_slinging_salter::base64SaltedHashFromBase64Key(
            id, hmacKey_);
    auto shard = getShardFor(base64SaltedId, numShards);
    *outFiles.at(shard) << base64SaltedId << line.substr(commaPos) << "\n";
  }
}
} // namespace data_processing::sharder
