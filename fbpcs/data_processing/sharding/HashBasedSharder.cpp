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
uint64_t ntohl_64(uint64_t in) {
  // There is not ntohl_64 function in stanard library.
  // We reuse ntohl to create ntohl_64.
  // swap 0-3 bytes to 4-7 bytes
  // move byte 7 to byte 4
  // move byte 5 to byte 6
  // move byte 6 to byte 5
  // move byte 4 to byte 7
  // move byte 3 to byte 0
  // move byte 1 to byte 2
  // move byte 2 to byte 1
  // move byte 0 to byte 3
  return ((uint64_t)ntohl((in)&0xFFFFFFFF) << 32) | ntohl((in) >> 32);
}

std::vector<uint8_t> toBytes(const std::string& key) {
  std::vector<uint8_t> res(key.begin(), key.end());
  return res;
}

uint64_t bytesToUInt64(const std::vector<uint8_t>& bytes) {
  uint64_t res = 0;

  auto bytesInSizeT = sizeof(res) / sizeof(uint8_t);
  auto end = bytes.begin() + std::min(bytesInSizeT, bytes.size());
  std::copy(bytes.begin(), end, reinterpret_cast<uint8_t*>(&res));

  // Because we could be in a bizarre scenario where the publisher machine's
  // endianness differs from the partner machine's endianness, we rearrange the
  // bytes now to ensure a consistent representation. We assume the previously
  // copied bytes are in "network byte order" and convert them to a host long.
  return ntohl_64(res);
}
} // namespace detail

std::size_t HashBasedSharder::getShardFor(
    const std::string& id,
    std::size_t numShards) {
  auto toInt = detail::bytesToUInt64(detail::toBytes(id));
  return toInt % numShards;
}

void HashBasedSharder::shardLine(
    std::string line,
    const std::vector<std::unique_ptr<fbpcf::io::BufferedWriter>>& outFiles,
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
  std::string lineToWrite = folly::join(",", cols);
  std::string newLine = "\n";
  outFiles.at(shard)->writeString(lineToWrite);
  outFiles.at(shard)->writeString(newLine);
  logRowsToShard(shard);
}
} // namespace data_processing::sharder
