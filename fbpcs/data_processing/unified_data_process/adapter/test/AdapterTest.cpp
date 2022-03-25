/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cmath>
#include <future>
#include <memory>
#include <random>
#include <unordered_map>

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/mpc_std_lib/permuter/AsWaksmanPermuterFactory.h"
#include "fbpcf/mpc_std_lib/permuter/DummyPermuterFactory.h"
#include "fbpcf/mpc_std_lib/shuffler/NonShufflerFactory.h"
#include "fbpcf/mpc_std_lib/shuffler/PermuteBasedShufflerFactory.h"
#include "fbpcf/scheduler/SchedulerHelper.h"
#include "fbpcf/test/TestHelper.h"
#include "fbpcs/data_processing/unified_data_process/adapter/AdapterFactory.h"

namespace unified_data_process::adapter {

std::vector<int64_t> generateShuffledIndex(size_t size) {
  std::vector<int64_t> rst(size);
  for (size_t i = 0; i < size; i++) {
    rst[i] = i;
  }
  std::random_shuffle(rst.begin(), rst.end());
  return rst;
}

std::tuple<
    std::vector<int64_t>,
    std::vector<int64_t>,
    std::unordered_map<int64_t, int64_t>>
generateAdapterTestData() {
  std::random_device rd;
  std::mt19937_64 e(rd());
  std::uniform_int_distribution<int32_t> randomUnionSize(3, 0xFF);
  std::uniform_int_distribution<uint8_t> randomElement(0, 2);

  auto unionSize = randomUnionSize(e);
  size_t p0InputSize = 0;
  size_t p1InputSize = 0;
  std::vector<uint8_t> u(unionSize);
  for (auto& item : u) {
    item = randomElement(e);
    if (item != 0) {
      p0InputSize++;
    }
    if (item != 1) {
      p1InputSize++;
    }
  }

  std::vector<int64_t> p0Data = generateShuffledIndex(p0InputSize);
  std::vector<int64_t> p1Data = generateShuffledIndex(p1InputSize);
  std::vector<int64_t> p0Input(unionSize);
  std::vector<int64_t> p1Input(unionSize);
  size_t p0Index = 0;
  size_t p1Index = 0;
  std::unordered_map<int64_t, int64_t> expectedOutput;
  for (size_t i = 0; i < unionSize; i++) {
    if (u.at(i) == 2) {
      expectedOutput[p0Data.at(p0Index)] = p1Data.at(p1Index);
    }

    if (u.at(i) != 0) {
      p0Input[i] = p0Data.at(p0Index);
      p0Index++;
    } else {
      p0Input[i] = -1;
    }
    if (u.at(i) != 1) {
      p1Input[i] = p1Data.at(p1Index);
      p1Index++;
    } else {
      p1Input[i] = -1;
    }
  }
  return {p0Input, p1Input, expectedOutput};
}

void checkOutput(
    const std::vector<int64_t>& p0Output,
    const std::vector<int64_t>& p1Output,
    const std::unordered_map<int64_t, int64_t>& expectedOutput) {
  ASSERT_EQ(p0Output.size(), expectedOutput.size());
  ASSERT_EQ(p1Output.size(), expectedOutput.size());
  for (size_t i = 0; i < p0Output.size(); i++) {
    ASSERT_TRUE(expectedOutput.find(p1Output.at(i)) != expectedOutput.end());
    EXPECT_EQ(expectedOutput.at(p1Output.at(i)), p0Output.at(i));
  }
}

void adapterTest(
    std::unique_ptr<IAdapter> adapter0,
    std::unique_ptr<IAdapter> adapter1) {
  auto [p0Input, p1Input, expectedOutput] = generateAdapterTestData();
  auto task = [](std::unique_ptr<IAdapter> adapter,
                 const std::vector<int64_t>& input) {
    return adapter->adapt(input);
  };
  auto future0 = std::async(task, std::move(adapter0), p0Input);
  auto future1 = std::async(task, std::move(adapter1), p1Input);
  auto rst0 = future0.get();
  auto rst1 = future1.get();
  checkOutput(rst0, rst1, expectedOutput);
}

TEST(AdapterTest, testAdapterWithNonShuffler) {
  auto agentFactories =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(*agentFactories[0], *agentFactories[1]);
  AdapterFactory<0> factory0(
      true,
      0,
      1,
      std::make_unique<
          fbpcf::mpc_std_lib::shuffler::insecure::NonShufflerFactory<
              fbpcf::frontend::BitString<true, 0, true>>>());

  AdapterFactory<1> factory1(
      false,
      0,
      1,
      std::make_unique<
          fbpcf::mpc_std_lib::shuffler::insecure::NonShufflerFactory<
              fbpcf::frontend::BitString<true, 1, true>>>());

  adapterTest(factory0.create(), factory1.create());
}

TEST(AdapterTest, testAdapterWithPermuteBasedShufflerAndDummyPermuter) {
  auto agentFactories =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(*agentFactories[0], *agentFactories[1]);
  AdapterFactory<0> factory0(
      true,
      0,
      1,
      std::make_unique<
          fbpcf::mpc_std_lib::shuffler::PermuteBasedShufflerFactory<
              fbpcf::frontend::BitString<true, 0, true>>>(
          0,
          1,
          std::make_unique<
              fbpcf::mpc_std_lib::permuter::insecure::DummyPermuterFactory<
                  fbpcf::frontend::BitString<true, 0, true>>>(0, 1),
          std::make_unique<fbpcf::engine::util::AesPrgFactory>()));

  AdapterFactory<1> factory1(
      false,
      0,
      1,
      std::make_unique<
          fbpcf::mpc_std_lib::shuffler::PermuteBasedShufflerFactory<
              fbpcf::frontend::BitString<true, 1, true>>>(
          1,
          0,
          std::make_unique<
              fbpcf::mpc_std_lib::permuter::insecure::DummyPermuterFactory<
                  fbpcf::frontend::BitString<true, 1, true>>>(1, 0),
          std::make_unique<fbpcf::engine::util::AesPrgFactory>()));

  adapterTest(factory0.create(), factory1.create());
}

TEST(AdapterTest, testAdapterWithSecurePermuteBasedShuffler) {
  auto agentFactories =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(*agentFactories[0], *agentFactories[1]);
  AdapterFactory<0> factory0(
      true,
      0,
      1,
      std::make_unique<
          fbpcf::mpc_std_lib::shuffler::PermuteBasedShufflerFactory<
              fbpcf::frontend::BitString<true, 0, true>>>(
          0,
          1,
          std::make_unique<fbpcf::mpc_std_lib::permuter::
                               AsWaksmanPermuterFactory<std::vector<bool>, 0>>(
              0, 1),
          std::make_unique<fbpcf::engine::util::AesPrgFactory>()));

  AdapterFactory<1> factory1(
      false,
      0,
      1,
      std::make_unique<
          fbpcf::mpc_std_lib::shuffler::PermuteBasedShufflerFactory<
              fbpcf::frontend::BitString<true, 1, true>>>(
          1,
          0,
          std::make_unique<fbpcf::mpc_std_lib::permuter::
                               AsWaksmanPermuterFactory<std::vector<bool>, 1>>(
              1, 0),
          std::make_unique<fbpcf::engine::util::AesPrgFactory>()));

  adapterTest(factory0.create(), factory1.create());
}

} // namespace unified_data_process::adapter
