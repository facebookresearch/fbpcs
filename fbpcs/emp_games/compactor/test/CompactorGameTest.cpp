/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/test/TestHelper.h"
#include "fbpcs/emp_games/compactor/CompactorGame.h"

namespace compactor {

const int8_t adIdWidth = 64;
const int8_t convWidth = 32;

using AttributionValue = std::pair<
    fbpcf::mpc_std_lib::util::Intp<false, adIdWidth>,
    fbpcf::mpc_std_lib::util::Intp<false, convWidth>>;

// run a compactor game on XOR secret share inputs.
template <class CompactorGame, int schedulerId>
std::tuple<std::vector<uint64_t>, std::vector<uint64_t>, std::vector<bool>>
runCompactorGame(
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator,
    int myId,
    int partnerId,
    const std::vector<AttributionOutputShare>& src,
    size_t size,
    bool shouldRevealSize) {
  auto scheduler = schedulerCreator(myId, *factory);

  auto game =
      std::make_unique<CompactorGame>(std::move(scheduler), myId, partnerId);

  SecretAttributionOutput<schedulerId> secret(src);

  auto compactified = game->play(secret, size, shouldRevealSize);

  auto rstAd = compactified.adId.openToParty(0).getValue();
  auto rstConv = compactified.conversionValue.openToParty(0).getValue();
  auto rstLabel = compactified.isAttributed.openToParty(0).getValue();

  return {rstAd, rstConv, rstLabel};
}

template <class CompactorGame0, class CompactorGame1>
void testCompactorGame(fbpcf::SchedulerType schedulerType) {
  // read secret share inputs from file
  std::string filename0 =
      "fbpcs/emp_games/compactor/test/test_input/publisher_test_output.csv_0";
  std::string filename1 =
      "fbpcs/emp_games/compactor/test/test_input/partner_test_output.csv_0";
  auto share0 = readXORShareInput(filename0);
  auto share1 = readXORShareInput(filename1);
  auto batchSize = share0.size();

  std::vector<std::tuple<uint64_t, uint64_t, bool>> expectedData;
  for (size_t i = 0; i < share0.size(); i++) {
    auto expectedAd = share0.at(i).adId ^ share1.at(i).adId;
    auto expectedConv =
        share0.at(i).conversionValue ^ share1.at(i).conversionValue;
    auto expectedLabel = share0.at(i).isAttributed ^ share1.at(i).isAttributed;
    if (expectedLabel) {
      expectedData.push_back({expectedAd, expectedConv, expectedLabel == 1});
    }
  }

  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);
  const bool unsafe = true;
  fbpcf::SchedulerCreator schedulerCreator =
      fbpcf::getSchedulerCreator<unsafe>(schedulerType);

  auto future0 = std::async(
      runCompactorGame<CompactorGame0, 0>,
      std::move(factories[0]),
      schedulerCreator,
      0,
      1,
      share0,
      batchSize,
      true);
  auto future1 = std::async(
      runCompactorGame<CompactorGame1, 1>,
      std::move(factories[1]),
      schedulerCreator,
      1,
      0,
      share1,
      batchSize,
      true);

  auto [rstAd, rstConv, rstLabel] = future0.get();
  future1.get();

  // check the correctness of the outputsize
  ASSERT_EQ(rstAd.size(), expectedData.size());
  ASSERT_EQ(rstConv.size(), expectedData.size());
  ASSERT_EQ(rstLabel.size(), expectedData.size());

  // verify values in each set by checking the existence in expectedData
  for (size_t i = 0; i < expectedData.size(); i++) {
    ASSERT_TRUE(
        std::find(
            expectedData.begin(),
            expectedData.end(),
            std::make_tuple(rstAd.at(i), rstConv.at(i), rstLabel.at(i))) !=
        expectedData.end());
  }
}

/* run the same tests with multiple schedulers */
class CompactorGameTestFixture
    : public ::testing::TestWithParam<fbpcf::SchedulerType> {};

TEST_P(CompactorGameTestFixture, TestShuffleBasedCompactorGame) {
  auto schedulerType = GetParam();
  testCompactorGame<
      ShuffleBasedCompactorGame<AttributionValue, 0>,
      ShuffleBasedCompactorGame<AttributionValue, 1>>(schedulerType);
}
TEST_P(CompactorGameTestFixture, TestNonShuffleBasedCompactorGame) {
  auto schedulerType = GetParam();
  testCompactorGame<
      NonShuffleBasedCompactorGame<AttributionValue, 0>,
      NonShuffleBasedCompactorGame<AttributionValue, 1>>(schedulerType);
}
TEST_P(CompactorGameTestFixture, TestDummyCompactorGame) {
  auto schedulerType = GetParam();
  testCompactorGame<
      DummyCompactorGame<AttributionValue, 0>,
      DummyCompactorGame<AttributionValue, 1>>(schedulerType);
}

INSTANTIATE_TEST_SUITE_P(
    CompactorGameTest,
    CompactorGameTestFixture,
    ::testing::Values(
        fbpcf::SchedulerType::NetworkPlaintext,
        fbpcf::SchedulerType::Eager,
        fbpcf::SchedulerType::Lazy),
    [](const testing::TestParamInfo<CompactorGameTestFixture::ParamType>&
           info) {
      auto schedulerType = info.param;
      return fbpcf::getSchedulerName(schedulerType);
    });
} // namespace compactor
