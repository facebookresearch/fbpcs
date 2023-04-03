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

DEFINE_int32(hashing_prime, 37, "Prime number to assist in consistent hashing");

namespace data_processing::sharder {
std::size_t hashString(const std::string& s, uint64_t hashing_prime) {
  std::size_t res = 0;
  for (auto i = 0; i < s.length(); ++i) {
    res = hashing_prime * res + s[i];
  }
  return res;
}

std::size_t HashBasedSharder::getShardFor(
    const std::string& id,
    std::size_t numShards) {
  auto hashed = hashString(id, FLAGS_hashing_prime); // returns std::size_t
  return hashed % numShards;
}

void HashBasedSharder::shardLine(
    std::string line,
    const std::vector<std::unique_ptr<fbpcf::io::BufferedWriter>>& outFiles,
    const std::vector<int32_t>& idColumnIndices) {
  std::vector<std::string> cols;
  folly::split(',', line, cols);

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
