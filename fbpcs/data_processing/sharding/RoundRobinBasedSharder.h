/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include "fbpcs/data_processing/sharding/GenericSharder.h"

namespace data_processing::sharder {
class RoundRobinBasedSharder final : public GenericSharder {
 public:
  /**
   * Create a new RoundRobinBasedSharder which simply shards a file by sending
   * each row to the next shard in a round-robin algorithm. Subsequent runs of
   * the same program will yield the same output.
   *
   * @param inputPath a path to the file to be sharded
   * @param outputPaths a list of paths to the output sharded files
   * @param logEveryN how often to log progress updates
   */
  RoundRobinBasedSharder(
      std::string inputPath,
      std::vector<std::string> outputPaths,
      int32_t logEveryN)
      : GenericSharder{inputPath, outputPaths, logEveryN} {}

  /**
   * Create a new RoundRobinBasedSharder which simply shards a file by sending
   * each row to the next shard in a round-robin algorithm. Subsequent runs of
   * the same program will yield the same output.
   *
   * @param inputPath a path to the file to be sharded
   * @param outputBasePath the prefix to use for all paths
   * @param startIndex the first subPath index to generate
   * @param endIndex the first subPath index to *not* generate (see gen)
   * @param logEveryN how often to log progress updates
   */
  RoundRobinBasedSharder(
      std::string inputPath,
      std::string outputBasePath,
      std::size_t startIndex,
      std::size_t endIndex,
      int32_t logEveryN)
      : GenericSharder{
            inputPath,
            outputBasePath,
            startIndex,
            endIndex,
            logEveryN} {}

  /**
   * Determine which shard a line should go to given an id. Internally, we just
   * increment idx_ and keep sharding to the *next* output path.
   *
   * @param id the identifier representing the line to be sharded
   * @param numShards the number of shards to be considered
   * @returns the shard this id should be sent to
   */
  std::size_t getShardFor(const std::string& id, std::size_t numShards) final;

 private:
  std::size_t idx_ = 0;
};
} // namespace data_processing::sharder
