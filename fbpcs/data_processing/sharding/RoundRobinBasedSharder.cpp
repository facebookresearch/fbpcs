/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/data_processing/sharding/RoundRobinBasedSharder.h"

namespace data_processing::sharder {
std::size_t RoundRobinBasedSharder::getShardFor(
    const std::string& /* unused */,
    std::size_t numShards) {
  auto res = idx_ % numShards;
  ++idx_;
  return res;
}
} // namespace data_processing::sharder
