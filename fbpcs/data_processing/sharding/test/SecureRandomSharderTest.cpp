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

#include "fbpcf/engine/util/AesPrgFactory.h"
#include "fbpcf/engine/util/util.h"
#include "fbpcs/data_processing/sharding/SecureRandomSharder.h"

namespace data_processing::sharder {

void testRandomSharder(size_t numberOfShards, int numberOfItem) {
  fbpcf::engine::util::AesPrgFactory aesPrgFactory;
  auto key = fbpcf::engine::util::getRandomM128iFromSystemNoise();

  SecureRandomSharder sharder1(
      "unused", "unused", 0, numberOfShards, 9, aesPrgFactory.create(key));

  SecureRandomSharder sharder2(
      "unused", "unused", 0, numberOfShards, 9, aesPrgFactory.create(key));

  std::vector<int> count(numberOfShards, 0);
  for (int i = 0; i < numberOfItem; i++) {
    auto id1 = sharder1.getShardFor("unused", 0 /*unused*/);
    auto id2 = sharder2.getShardFor("unused", 0 /*unused*/);
    EXPECT_EQ(id1, id2);
    ASSERT_LT(id1, numberOfShards);
    count.at(id1)++;
  }
  double p = 1.0 / numberOfShards;
  double expectation = p * numberOfItem;
  double standDev = std::sqrt(numberOfItem * p * (1 - p));
  for (auto item : count) {
    // the probablity of fall out of 4 standdev is 1 in 15k.
    EXPECT_LE(item, expectation + 4 * standDev);
    EXPECT_GE(item, expectation - 4 * standDev);
  }
}

TEST(SecureRandomSharderTest, TestGetShardFor) {
  testRandomSharder(100, 50000);
}

} // namespace data_processing::sharder
