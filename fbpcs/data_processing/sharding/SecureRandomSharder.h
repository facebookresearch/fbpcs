/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "fbpcf/engine/util/IPrg.h"
#include "fbpcs/data_processing/sharding/GenericSharder.h"

namespace data_processing::sharder {
class SecureRandomSharder final : public GenericSharder {
 public:
  /**
   * Create a new SecureRandomSharder which simply shards a file by sending
   * each row to the next shard randomly. With the same randomness from the prg,
   * the output will be exactly the same as well.
   *
   * @param inputPath a path to the file to be sharded
   * @param outputPaths a list of paths to the output sharded files
   * @param logEveryN how often to log progress updates
   */
  SecureRandomSharder(
      std::string inputPath,
      std::vector<std::string> outputPaths,
      int32_t logEveryN,
      std::unique_ptr<fbpcf::engine::util::IPrg> prg)
      : GenericSharder{inputPath, outputPaths, logEveryN},
        prg_(std::move(prg)),
        numShards_(getOutputPaths().size()) {
    ctx_ = BN_CTX_new();
    if (ctx_ == nullptr) {
      throw std::runtime_error(
          "BN_CTX initialization failed: " + std::to_string(ERR_get_error()));
    }
  }

  /**
   * Create a new SecureRandomSharder which simply shards a file by sending
   * each row to the next shard in a round-robin algorithm. Subsequent runs of
   * the same program will yield the same output.
   *
   * @param inputPath a path to the file to be sharded
   * @param outputBasePath the prefix to use for all paths
   * @param startIndex the first subPath index to generate
   * @param endIndex the first subPath index to *not* generate (see gen)
   * @param logEveryN how often to log progress updates
   */
  SecureRandomSharder(
      std::string inputPath,
      std::string outputBasePath,
      std::size_t startIndex,
      std::size_t endIndex,
      int32_t logEveryN,
      std::unique_ptr<fbpcf::engine::util::IPrg> prg)
      : GenericSharder{inputPath, outputBasePath, startIndex, endIndex, logEveryN},
        prg_(std::move(prg)),
        numShards_(getOutputPaths().size()) {
    ctx_ = BN_CTX_new();
    if (ctx_ == nullptr) {
      throw std::runtime_error(
          "BN_CTX initialization failed: " + std::to_string(ERR_get_error()));
    }
  }

  ~SecureRandomSharder() {
    BN_CTX_free(ctx_);
  }

  /**
   * Determine which shard a line should go to given an id.
   *
   * @param id the identifier representing the line to be sharded
   * @param numShards the number of shards to be considered
   * @returns the shard this id should be sent to
   */
  std::size_t getShardFor(const std::string& id, std::size_t numShards) final;

 private:
  std::unique_ptr<fbpcf::engine::util::IPrg> prg_;
  BN_CTX* ctx_;
  size_t numShards_;
};

} // namespace data_processing::sharder
