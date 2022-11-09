/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <fbpcf/io/api/BufferedWriter.h>
#include "fbpcs/data_processing/sharding/GenericSharder.h"

namespace data_processing::sharder {
/* Utility to hash a string to an unsigned machine size integer.
 * Unsigned is important so overflow is properly defined.
 * Adapted from
 * https://stackoverflow.com/questions/8567238/hash-function-in-c-for-string-to-int
 */
std::size_t hashString(const std::string& s, uint64_t hashing_prime);

class HashBasedSharder final : public GenericSharder {
 public:
  /**
   * Create a new HashBasedSharder which is able to consistently hash a line
   * by interpreting the (hashed) identifier as a range of bytes and then using
   * modulo arithmetic to map it to a shard. Subsequent runs of the same program
   * will yield the same output.
   *
   * @param inputPath a path to the file to be sharded
   * @param outputPaths a list of paths to the output sharded files
   * @param logEveryN how often to log progress updates
   * @param hmacKey an optional key to be used if this sharder will be doing the
   *     HMAC_SHA256 operation (usually this is done beforehand upstream)
   */
  HashBasedSharder(
      std::string inputPath,
      std::vector<std::string> outputPaths,
      int32_t logEveryN,
      std::string hmacKey)
      : GenericSharder{inputPath, outputPaths, logEveryN},
        hmacKey_{std::move(hmacKey)} {}

  /**
   * Create a new HashBasedSharder which is able to consistently hash a line
   * by interpreting the (hashed) identifier as a range of bytes and then using
   * modulo arithmetic to map it to a shard. Subsequent runs of the same program
   * will yield the same output.
   *
   * @param inputPath a path to the file to be sharded
   * @param outputBasePath the prefix to use for all paths
   * @param startIndex the first subPath index to generate
   * @param endIndex the first subPath index to *not* generate (see gen)
   * @param logEveryN how often to log progress updates
   * @param hmacKey an optional key to be used if this sharder will be doing the
   *     HMAC_SHA256 operation (usually this is done beforehand upstream)
   */
  HashBasedSharder(
      std::string inputPath,
      std::string outputBasePath,
      std::size_t startIndex,
      std::size_t endIndex,
      int32_t logEveryN,
      std::string hmacKey)
      : GenericSharder{inputPath, outputBasePath, startIndex, endIndex, logEveryN},
        hmacKey_{std::move(hmacKey)} {}

  /**
   * Get the correct shard associated with a string.
   *
   * @param id the id to be sharded
   * @param numShards the total number of shards being created
   * @returns the shard index this identifier belongs to
   */
  std::size_t getShardFor(const std::string& id, std::size_t numShards) final;

  /**
   * Shard an input line by hashing each identifier into an int32_t first using
   * a hashing method that works on both big- and little-endian machines.
   *
   * @param line the line to be sharded
   * @param outFiles the list of output files to be sharded into
   */
  void shardLine(
      std::string line,
      const std::vector<std::unique_ptr<fbpcf::io::BufferedWriter>>& outFiles,
      const std::vector<int32_t>& idColumnIndices) final;

 private:
  std::string hmacKey_;
};
} // namespace data_processing::sharder
