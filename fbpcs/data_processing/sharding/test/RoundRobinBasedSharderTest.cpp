/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "fbpcs/data_processing/sharding/RoundRobinBasedSharder.h"

namespace data_processing::sharder {
TEST(RoundRobinBasedSharderTest, TestGetShardFor) {
  RoundRobinBasedSharder sharder{"unused", {/* unused */}, 123};
  EXPECT_EQ(0, sharder.getShardFor("foo", 2));
  EXPECT_EQ(1, sharder.getShardFor("bar", 2));
  EXPECT_EQ(0, sharder.getShardFor("baz", 2));
  EXPECT_EQ(1, sharder.getShardFor("quux", 2));
}
} // namespace data_processing::sharder
