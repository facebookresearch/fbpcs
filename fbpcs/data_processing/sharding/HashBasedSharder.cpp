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

#include <folly/logging/xlog.h>

#include "fbpcs/data_processing/hash_slinging_salter/HashSlingingSalter.hpp"
#include "folly/String.h"

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
    const std::vector<std::unique_ptr<std::ofstream>>& outFiles,
    const std::vector<int32_t>& idColumnIndices) {
  std::vector<std::string> cols;
  folly::split(",", line, cols);

  std::string id = "";
  for (auto idColumnIdx : idColumnIndices) {
    if (idColumnIdx >= cols.size()) {
      XLOG_EVERY_MS(INFO, 5000)
          << "Discrepancy with header:" << line << " does not have "
          << idColumnIdx << "th column.\n";
      return;
    }
    auto& col = cols.at(idColumnIdx);
    if (!col.empty()) {
      if (!hmacKey_.empty()) {
        // If hmacBase64Key is empty, the hashing already happened upstream.
        // This means we can reinterpret the id as a base64-encoded string.
        // Otherwise, hash all the id columns.
        col = private_lift::hash_slinging_salter::base64SaltedHashFromBase64Key(
            col, hmacKey_);
      }
      if (id.empty()) {
        id = col;
      }
    }
  }
  if (id.empty()) {
    XLOG_EVERY_MS(INFO, 5000) << "All the id values are empty in this row";
    return;
  }
  auto numShards = outFiles.size();
  std::size_t shard;
  shard = getShardFor(id, numShards);
  *outFiles.at(shard) << folly::join(",", cols) << "\n";
  logRowsToShard(shard);
}
} // namespace data_processing::sharder
