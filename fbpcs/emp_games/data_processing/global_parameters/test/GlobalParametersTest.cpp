/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/global_parameters/GlobalParameters.h"
#include <gtest/gtest.h>
#include "folly/Format.h"
#include "folly/Random.h"

using namespace ::testing;

namespace global_parameters {

TEST(GlobalParametersSerialization, testSerializeAndDeserialize) {
  const std::string file =
      folly::sformat("./global_parameter_{}", folly::Random::rand32());

  GlobalParameters gp;
  gp.emplace("test1", 3);
  gp.emplace(
      "test2", std::unordered_map<int32_t, int32_t>({{1, 2}, {3, 4}, {5, 6}}));

  writeToFile(file, gp);
  auto gp1 = readFromFile(file);
  std::remove(file.c_str());

  EXPECT_EQ(
      boost::get<int32_t>(gp.at("test1")),
      boost::get<int32_t>(gp1.at("test1")));

  EXPECT_EQ(
      (boost::get<std::unordered_map<int32_t, int32_t>>(gp.at("test2"))),
      (boost::get<std::unordered_map<int32_t, int32_t>>(gp1.at("test2"))));
}

} // namespace global_parameters
