/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <memory>
#include <set>

#include <fbpcs/emp_games/pcf2_aggregation/AttributionReformattedResult.h>
#include "folly/test/JsonTestUtil.h"

#include "fbpcf/engine/communication/InMemoryPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/io/FileManagerUtil.h"
#include "fbpcf/scheduler/PlaintextScheduler.h"
#include "fbpcf/scheduler/WireKeeper.h"
#include "fbpcs/emp_games/common/TestUtil.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/test/TestUtils.h"
#include "fbpcs/emp_games/pcf2_aggregation/AggregationGame.h"
#include "fbpcs/emp_games/pcf2_aggregation/AggregationOptions.h"
#include "fbpcs/emp_games/pcf2_aggregation/test/AggregationTestUtils.h"

namespace pcf2_aggregation {

const bool unsafe = true;

TEST(AggregationGameTest, TestShareAggregationFormats) {
  std::vector<std::string> aggregationFormatNames = {common::MEASUREMENT};

  AggregationGame<common::PUBLISHER> game(
      std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()),
      std::move(fbpcf::engine::communication::getInMemoryAgentFactory(1)[0]),
      common::InputEncryption::Plaintext);

  auto aggregationFormats =
      game.shareAggregationFormats(common::PUBLISHER, aggregationFormatNames);

  ASSERT_EQ(aggregationFormats.size(), 1);
  EXPECT_EQ(aggregationFormats.at(0).name, common::MEASUREMENT);
}

TEST(AggregationGameTest, TestPrivateMeasurementTouchpointMetadataPlaintext) {
  std::vector<std::vector<TouchpointMetadata>> touchpointMetadata{
      std::vector<TouchpointMetadata>{
          TouchpointMetadata{0, 8000, true, 0, 0},
          TouchpointMetadata{255, 5000, true, 20, 255},
          TouchpointMetadata{100, 20000, false, 0, 100}}};

  AggregationGame<common::PUBLISHER> game(
      std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()),
      std::move(fbpcf::engine::communication::getInMemoryAgentFactory(1)[0]),
      common::InputEncryption::Plaintext);

  auto privateTouchpointMetadata =
      game.privatelyShareMeasurementTouchpointMetadata(touchpointMetadata)
          .at(0);

  ASSERT_EQ(privateTouchpointMetadata.size(), 3);

  EXPECT_EQ(
      privateTouchpointMetadata.at(0)
          .adId.openToParty(common::PARTNER)
          .getValue(),
      0);
  EXPECT_EQ(
      privateTouchpointMetadata.at(1)
          .adId.openToParty(common::PARTNER)
          .getValue(),
      255);
  EXPECT_EQ(
      privateTouchpointMetadata.at(2)
          .adId.openToParty(common::PARTNER)
          .getValue(),
      100);
}

TEST(AggregationGameTest, TestPrivateMeasurementConversionPlaintext) {
  common::InputEncryption inputEncryption = common::InputEncryption::Plaintext;

  std::vector<std::vector<ConversionMetadata>> conversionMetadata{
      std::vector<ConversionMetadata>{
          ConversionMetadata{10000, 5000, 0, inputEncryption},
          ConversionMetadata{100, 0, 0, inputEncryption},
          ConversionMetadata{0, 1000, 20, inputEncryption}}};

  AggregationGame<common::PUBLISHER> game(
      std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()),
      std::move(fbpcf::engine::communication::getInMemoryAgentFactory(1)[0]),
      inputEncryption);

  auto privateConversionMetadata =
      game.privatelyShareMeasurementConversionMetadata(conversionMetadata)
          .at(0);

  ASSERT_EQ(privateConversionMetadata.size(), 3);

  EXPECT_EQ(
      privateConversionMetadata.at(0)
          .convValue.openToParty(common::PUBLISHER)
          .getValue(),
      5000);
  EXPECT_EQ(
      privateConversionMetadata.at(1)
          .convValue.openToParty(common::PUBLISHER)
          .getValue(),
      0);
  EXPECT_EQ(
      privateConversionMetadata.at(2)
          .convValue.openToParty(common::PUBLISHER)
          .getValue(),
      1000);
}

// Helper method to share attribution results and open to one party with
// scheduler
template <int schedulerId>
std::vector<std::vector<bool>> shareAttributionResultsWithScheduler(
    int myId,
    std::vector<std::vector<AttributionResult>> attributionResults,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  // share attribution results
  auto scheduler = schedulerCreator(myId, *factory);
  auto game = std::make_unique<AggregationGame<schedulerId>>(
      std::move(scheduler),
      std::move(factory),
      common::InputEncryption::Plaintext);
  auto privateAttributionResults =
      game->privatelyShareAttributionResults(attributionResults).at(0);

  // open results
  std::vector<std::vector<bool>> output{std::vector<bool>{}};
  for (size_t i = 0; i < privateAttributionResults.size(); ++i) {
    output.at(0).push_back(privateAttributionResults.at(i)
                               .isAttributed.openToParty(common::PUBLISHER)
                               .getValue());
  }
  return output;
}

template <int schedulerId>
std::vector<std::vector<bool>> shareAttributionReformattedResultsWithScheduler(
    int myId,
    std::vector<std::vector<AttributionReformattedResult>>
        attributionReformattedResults,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  FLAGS_use_new_output_format = true;
  // share attribution results
  auto scheduler = schedulerCreator(myId, *factory);
  auto game = std::make_unique<AggregationGame<schedulerId>>(
      std::move(scheduler),
      std::move(factory),
      common::InputEncryption::Plaintext);
  auto privateAttributionReformattedResults =
      game->privatelyShareAttributionReformattedResults(
              attributionReformattedResults)
          .at(0);

  // open results
  std::vector<std::vector<bool>> output{std::vector<bool>{}};
  for (size_t i = 0; i < privateAttributionReformattedResults.size(); ++i) {
    output.at(0).push_back(privateAttributionReformattedResults.at(i)
                               .isAttributed.openToParty(common::PUBLISHER)
                               .getValue());
  }
  return output;
}

void testAttributionResultWithScheduler(
    fbpcf::SchedulerCreator schedulerCreator) {
  std::vector<std::vector<AttributionResult>> publisherAttributionResult{
      std::vector<AttributionResult>{
          AttributionResult{true},
          AttributionResult{true},
          AttributionResult{false},
          AttributionResult{false}}};

  std::vector<std::vector<AttributionResult>> partnerAttributionResult{
      std::vector<AttributionResult>{
          AttributionResult{true},
          AttributionResult{false},
          AttributionResult{true},
          AttributionResult{false}}};

  // share results and open to one party
  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

  auto future0 = std::async(
      shareAttributionResultsWithScheduler<common::PUBLISHER>,
      common::PUBLISHER,
      publisherAttributionResult,
      std::move(factories[common::PUBLISHER]),
      schedulerCreator);

  auto future1 = std::async(
      shareAttributionResultsWithScheduler<common::PARTNER>,
      common::PARTNER,
      partnerAttributionResult,
      std::move(factories[common::PARTNER]),
      schedulerCreator);

  auto res0 = future0.get().at(0);
  auto res1 = future1.get().at(0);

  // check against expected output
  std::vector<bool> expectedOutput{false, true, true, false};

  ASSERT_EQ(res0.size(), 4);
  ASSERT_EQ(res1.size(), 4);

  for (size_t i = 0; i < res0.size(); ++i) {
    EXPECT_EQ(res0.at(i), expectedOutput.at(i));
  }
}

TEST(AggregationGameTest, TestAttributionResultNetworkPlaintextScheduler) {
  testAttributionResultWithScheduler(
      fbpcf::scheduler::createNetworkPlaintextScheduler<unsafe>);
}

TEST(AggregationGameTest, TestAttributionResultEagerScheduler) {
  testAttributionResultWithScheduler(
      fbpcf::scheduler::createEagerSchedulerWithInsecureEngine<unsafe>);
}

TEST(AggregationGameTest, TestAttributionResultLazyScheduler) {
  testAttributionResultWithScheduler(
      fbpcf::scheduler::createLazySchedulerWithInsecureEngine<unsafe>);
}

void testAttributionReformattedResultWithScheduler(
    fbpcf::SchedulerCreator schedulerCreator) {
  std::vector<std::vector<AttributionReformattedResult>>
      publisherAttributionResult{std::vector<AttributionReformattedResult>{
          AttributionReformattedResult{1, 20, true},
          AttributionReformattedResult{2, 30, true},
          AttributionReformattedResult{0, 40, false},
          AttributionReformattedResult{0, 60, false}}};

  std::vector<std::vector<AttributionReformattedResult>>
      partnerAttributionResult{std::vector<AttributionReformattedResult>{
          AttributionReformattedResult{1, 20, true},
          AttributionReformattedResult{0, 40, false},
          AttributionReformattedResult{3, 60, true},
          AttributionReformattedResult{0, 80, false}}};

  // share results and open to one party
  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

  auto future0 = std::async(
      shareAttributionReformattedResultsWithScheduler<common::PUBLISHER>,
      common::PUBLISHER,
      publisherAttributionResult,
      std::move(factories[common::PUBLISHER]),
      schedulerCreator);

  auto future1 = std::async(
      shareAttributionReformattedResultsWithScheduler<common::PARTNER>,
      common::PARTNER,
      partnerAttributionResult,
      std::move(factories[common::PARTNER]),
      schedulerCreator);

  auto res0 = future0.get().at(0);
  auto res1 = future1.get().at(0);

  // check against expected output
  std::vector<bool> expectedOutput{false, true, true, false};

  ASSERT_EQ(res0.size(), 4);
  ASSERT_EQ(res1.size(), 4);

  for (size_t i = 0; i < res0.size(); ++i) {
    EXPECT_EQ(res0.at(i), expectedOutput.at(i));
  }
}

TEST(
    AggregationGameTest,
    TestAttributionReformattedResultNetworkPlaintextScheduler) {
  testAttributionReformattedResultWithScheduler(
      fbpcf::scheduler::createNetworkPlaintextScheduler<unsafe>);
}

TEST(AggregationGameTest, TestAttributionReformattedResultEagerScheduler) {
  testAttributionReformattedResultWithScheduler(
      fbpcf::scheduler::createEagerSchedulerWithInsecureEngine<unsafe>);
}

TEST(AggregationGameTest, TestAttributionReformattedResultLazyScheduler) {
  testAttributionReformattedResultWithScheduler(
      fbpcf::scheduler::createLazySchedulerWithInsecureEngine<unsafe>);
}

// Helper method to share attribution results and open to one party with
// scheduler
template <int schedulerId>
std::vector<uint64_t> retrieveValidAdIdsWithSchedulerAndRealEngine(
    int myId,
    std::vector<std::vector<TouchpointMetadata>> tpmArrays,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto scheduler = schedulerCreator(myId, *factory);
  auto game = std::make_unique<AggregationGame<schedulerId>>(
      std::move(scheduler),
      std::move(factory),
      common::InputEncryption::Plaintext);
  return game->retrieveValidOriginalAdIds(myId, tpmArrays);
}

void testRetrieveValidAdIdsWithScheduler(
    std::unique_ptr<fbpcf::scheduler::IScheduler> schedulerCreator(
        int myId,
        fbpcf::engine::communication::IPartyCommunicationAgentFactory&
            communicationAgentFactory)) {
  std::vector<std::vector<TouchpointMetadata>> publisherTouchpointMetadata{
      std::vector<TouchpointMetadata>{
          TouchpointMetadata{0, 8000, true, 100, 0},
          TouchpointMetadata{2, 5000, false, 20, 2}},
      std::vector<TouchpointMetadata>{
          TouchpointMetadata{2, 10000, true, 10, 2},
          TouchpointMetadata{3, 20000, true, 50, 3}}};

  std::vector<std::vector<TouchpointMetadata>> partnerTouchpointMetadata{
      std::vector<TouchpointMetadata>{
          TouchpointMetadata{0, 0, false, 0, 0},
          TouchpointMetadata{0, 0, false, 0, 0}},
      std::vector<TouchpointMetadata>{
          TouchpointMetadata{0, 0, false, 0, 0},
          TouchpointMetadata{0, 0, false, 0, 0}}};

  // share results and open to one party
  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

  auto future0 = std::async(
      retrieveValidAdIdsWithSchedulerAndRealEngine<common::PUBLISHER>,
      common::PUBLISHER,
      publisherTouchpointMetadata,
      std::move(factories[common::PUBLISHER]),
      schedulerCreator);

  auto future1 = std::async(
      retrieveValidAdIdsWithSchedulerAndRealEngine<common::PARTNER>,
      common::PARTNER,
      partnerTouchpointMetadata,
      std::move(factories[common::PARTNER]),
      schedulerCreator);

  auto res0 = future0.get();
  auto res1 = future1.get();

  // check against expected output
  std::set<uint64_t> expectedOutput{2, 3};
  std::set<uint64_t> output0(res0.begin(), res0.end());
  std::set<uint64_t> output1(res1.begin(), res1.end());

  EXPECT_EQ(output0, expectedOutput);
  EXPECT_EQ(output1, expectedOutput);
}

TEST(AggregationGameTest, TestRetrieveValidAdIdsNetworkPlaintextScheduler) {
  testRetrieveValidAdIdsWithScheduler(
      fbpcf::scheduler::createNetworkPlaintextScheduler<unsafe>);
}

TEST(AggregationGameTest, TestRetrieveValidAdIdsEagerScheduler) {
  testRetrieveValidAdIdsWithScheduler(
      fbpcf::scheduler::createEagerSchedulerWithInsecureEngine<unsafe>);
}

TEST(AggregationGameTest, TestRetrieveValidAdIdsLazyScheduler) {
  testRetrieveValidAdIdsWithScheduler(
      fbpcf::scheduler::createLazySchedulerWithInsecureEngine<unsafe>);
}

template <int schedulerId>
AggregationOutputMetrics computeAggregationsWithScheduler(
    int myId,
    AggregationInputMetrics inputData,
    common::InputEncryption inputEncryption,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto scheduler = schedulerCreator(myId, *factory);
  auto game = std::make_unique<AggregationGame<schedulerId>>(
      std::move(scheduler), std::move(factory), inputEncryption);
  return game->computeAggregations(myId, inputData);
}

// Test cases are from https://fb.quip.com/IUHDApxKEAli
void testCorrectnessWithScheduler(
    common::InputEncryption inputEncryption,
    fbpcf::SchedulerCreator schedulerCreator) {
  std::string baseDir_ =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);
  // Attribution rules to test
  std::vector<std::string> attributionRules{
      common::LAST_CLICK_1D,
      common::LAST_TOUCH_1D,
      common::LAST_CLICK_2_7D,
      common::LAST_TOUCH_2_7D};
  // Currently only one aggregation format - measurement.
  std::vector<std::string> aggregationFormats{common::MEASUREMENT};

  for (auto attributionRule : attributionRules) {
    for (auto aggregationFormat : aggregationFormats) {
      std::string filePrefix =
          baseDir_ + "test_correctness/" + attributionRule + ".";
      std::string outputJsonFileName = filePrefix + aggregationFormat + ".json";
      std::string publisherSecretShareFileName = filePrefix + "publisher.json";
      std::string partnerSecretShareFileName = filePrefix + "partner.json";
      std::string clearTextFilePrefix = baseDir_ +
          "../../pcf2_attribution/test/test_correctness/" + attributionRule +
          ".";
      if (inputEncryption == common::InputEncryption::PartnerXor) {
        clearTextFilePrefix = clearTextFilePrefix + "partner_xor.";
      } else if (inputEncryption == common::InputEncryption::Xor) {
        clearTextFilePrefix = clearTextFilePrefix + "xor.";
      }
      std::string publisherClearTextFileName =
          clearTextFilePrefix + "publisher.csv";
      std::string partnerClearTextFileName =
          clearTextFilePrefix + "partner.csv";

      // read input files
      AggregationInputMetrics publisherInputData{
          common::PUBLISHER,
          inputEncryption,
          publisherSecretShareFileName,
          publisherClearTextFileName,
          aggregationFormat};
      AggregationInputMetrics partnerInputData{
          common::PARTNER,
          inputEncryption,
          partnerSecretShareFileName,
          partnerClearTextFileName,
          ""};

      // compute aggregations
      auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

      auto future0 = std::async(
          computeAggregationsWithScheduler<0>,
          0,
          publisherInputData,
          inputEncryption,
          std::move(factories[0]),
          schedulerCreator);

      auto future1 = std::async(
          computeAggregationsWithScheduler<1>,
          1,
          partnerInputData,
          inputEncryption,
          std::move(factories[1]),
          schedulerCreator);

      auto res0 = future0.get();
      auto res1 = future1.get();

      // check against expected output
      auto output =
          revealXORedResult(res0, res1, aggregationFormat, attributionRule);
      verifyOutput(output, outputJsonFileName);
    }
  }
}

class AggregationGameTestFixture
    : public ::testing::TestWithParam<
          std::tuple<common::SchedulerType, common::InputEncryption>> {};

TEST_P(AggregationGameTestFixture, TestCorrectness) {
  auto [schedulerType, inputEncryption] = GetParam();

  testCorrectnessWithScheduler(
      inputEncryption, fbpcf::getSchedulerCreator<unsafe>(schedulerType));
}

INSTANTIATE_TEST_SUITE_P(
    AggregationGameTest,
    AggregationGameTestFixture,
    ::testing::Combine(
        ::testing::Values(
            common::SchedulerType::NetworkPlaintext,
            common::SchedulerType::Eager,
            common::SchedulerType::Lazy),
        ::testing::Values(
            common::InputEncryption::Plaintext,
            common::InputEncryption::PartnerXor,
            common::InputEncryption::Xor)),
    [](const testing::TestParamInfo<AggregationGameTestFixture::ParamType>&
           info) {
      auto schedulerType = std::get<0>(info.param);
      auto inputEncryption = std::get<1>(info.param);

      return getSchedulerName(schedulerType) +
          getInputEncryptionString(inputEncryption);
    });
} // namespace pcf2_aggregation
