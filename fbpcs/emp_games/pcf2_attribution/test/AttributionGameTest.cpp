/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <future>

#include "folly/dynamic.h"
#include "folly/json.h"
#include "folly/logging/Init.h"
#include "folly/logging/xlog.h"
#include "folly/test/JsonTestUtil.h"

#include "fbpcf/io/FileManagerUtil.h"

#include "fbpcf/engine/communication/InMemoryPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/scheduler/PlaintextScheduler.h"
#include "fbpcf/scheduler/WireKeeper.h"
#include "fbpcf/test/TestHelper.h"
#include "fbpcs/emp_games/common/TestUtil.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/test/TestUtils.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionGame.h"
#include "fbpcs/emp_games/pcf2_attribution/AttributionOptions.h"
#include "fbpcs/emp_games/pcf2_attribution/test/AttributionTestUtils.h"

namespace pcf2_attribution {

const bool unsafe = true;

TEST(AttributionGameTest, TestPrivateTouchpointPlaintext) {
  std::vector<std::vector<Touchpoint<false>>> touchpoints{
      std::vector<Touchpoint<false>>{
          Touchpoint<false>{0, true, 100},
          Touchpoint<false>{1, false, 50},
          Touchpoint<false>{2, true, 0}}};

  AttributionGame<common::PUBLISHER, false, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto privateTouchpoints = game.privatelyShareTouchpoints(touchpoints).at(0);

  ASSERT_EQ(privateTouchpoints.size(), 3);

  EXPECT_EQ(
      privateTouchpoints.at(0).ts.openToParty(common::PUBLISHER).getValue(),
      100);
  EXPECT_EQ(
      privateTouchpoints.at(1).ts.openToParty(common::PUBLISHER).getValue(),
      50);
  EXPECT_EQ(
      privateTouchpoints.at(2).ts.openToParty(common::PUBLISHER).getValue(), 0);
}

TEST(AttributionGameTest, TestPrivateTouchpointPlaintextBatch) {
  std::vector<uint64_t> timestamp0{100, 50, 0};
  std::vector<uint64_t> timestamp1{99, 49, 3};

  std::vector<Touchpoint<true>> touchpoints{
      Touchpoint<true>{
          .id = {0, 1, 2},
          .isClick = {true, false, true},
          .ts = timestamp0,
      },
      Touchpoint<true>{
          .id = {3, 4, 5},
          .isClick = {false, true, false},
          .ts = timestamp1,
      }};

  AttributionGame<common::PUBLISHER, true, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto privateTouchpoints = game.privatelyShareTouchpoints(touchpoints);

  ASSERT_EQ(privateTouchpoints.size(), 2);

  fbpcf::testVectorEq<int64_t>(privateTouchpoints[0].id, {0, 1, 2});
  fbpcf::testVectorEq<int64_t>(privateTouchpoints[1].id, {3, 4, 5});

  auto sharedTimestamp0 =
      privateTouchpoints[0].ts.openToParty(common::PUBLISHER).getValue();
  auto sharedTimestamp1 =
      privateTouchpoints[1].ts.openToParty(common::PUBLISHER).getValue();

  fbpcf::testVectorEq<uint64_t>(timestamp0, sharedTimestamp0);
  fbpcf::testVectorEq<uint64_t>(timestamp1, sharedTimestamp1);
}

TEST(AttributionGameTest, TestPrivateConversionPlaintext) {
  std::vector<std::vector<Conversion<false>>> conversions{
      std::vector<Conversion<false>>{
          Conversion<false>{50},
          Conversion<false>{1000},
          Conversion<false>{0}}};

  AttributionGame<common::PUBLISHER, false, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto privateConversions = game.privatelyShareConversions(conversions).at(0);

  ASSERT_EQ(privateConversions.size(), 3);

  EXPECT_EQ(
      privateConversions.at(0).ts.openToParty(common::PUBLISHER).getValue(),
      50);
  EXPECT_EQ(
      privateConversions.at(1).ts.openToParty(common::PUBLISHER).getValue(),
      1000);
  EXPECT_EQ(
      privateConversions.at(2).ts.openToParty(common::PUBLISHER).getValue(), 0);
}

TEST(AttributionGameTest, TestPrivateConversionPlaintextBatch) {
  std::vector<uint64_t> timestamp0{100, 50, 0};
  std::vector<uint64_t> timestamp1{99, 49, 3};

  std::vector<Conversion<true>> conversions{
      Conversion<true>{.ts = timestamp0}, Conversion<true>{.ts = timestamp1}};

  AttributionGame<common::PUBLISHER, true, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto privateConversions = game.privatelyShareConversions(conversions);

  ASSERT_EQ(privateConversions.size(), 2);

  auto sharedTimestamp0 =
      privateConversions.at(0).ts.openToParty(common::PUBLISHER).getValue();
  auto sharedTimestamp1 =
      privateConversions.at(1).ts.openToParty(common::PUBLISHER).getValue();
  fbpcf::testVectorEq<uint64_t>(sharedTimestamp0, timestamp0);
  fbpcf::testVectorEq<uint64_t>(sharedTimestamp1, timestamp1);
}

TEST(AttributionGameTest, TestShareAttributionRules) {
  std::vector<std::string> attributionRuleNames = {
      common::LAST_CLICK_1D,
      common::LAST_TOUCH_1D,
      common::LAST_CLICK_28D,
      common::LAST_TOUCH_28D,
      common::LAST_CLICK_2_7D,
      common::LAST_TOUCH_2_7D,
      common::LAST_CLICK_1D_TARGETID};

  AttributionGame<common::PUBLISHER, false, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto attributionRules =
      game.shareAttributionRules(common::PUBLISHER, attributionRuleNames);

  ASSERT_EQ(attributionRules.size(), 7);
  EXPECT_EQ(attributionRules[0]->name, common::LAST_CLICK_1D);
  EXPECT_EQ(attributionRules[1]->name, common::LAST_TOUCH_1D);
  EXPECT_EQ(attributionRules[2]->name, common::LAST_CLICK_28D);
  EXPECT_EQ(attributionRules[3]->name, common::LAST_TOUCH_28D);
  EXPECT_EQ(attributionRules[4]->name, common::LAST_CLICK_2_7D);
  EXPECT_EQ(attributionRules[5]->name, common::LAST_TOUCH_2_7D);
  EXPECT_EQ(attributionRules[6]->name, common::LAST_CLICK_1D_TARGETID);
}

TEST(AttributionGameTest, TestAttributionLogicPlaintext) {
  std::vector<std::vector<Touchpoint<false>>> touchpoints{
      std::vector<Touchpoint<false>>{
          Touchpoint<false>{0, false, 125},
          Touchpoint<false>{1, true, 100},
          Touchpoint<false>{2, true, 200}}};

  std::vector<std::vector<Conversion<false>>> conversions{
      std::vector<Conversion<false>>{
          Conversion<false>{50},
          Conversion<false>{150},
          Conversion<false>{87000}}};

  AttributionGame<common::PUBLISHER, false, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto privateTouchpoints = game.privatelyShareTouchpoints(touchpoints);
  auto privateConversions = game.privatelyShareConversions(conversions);

  std::vector<bool> attributionResultsLastClick1D{
      /* conv 50 */ false,
      false,
      false,
      /* conv 150 */ false,
      true,
      false,
      /* conv 87000 */ false,
      false,
      false};

  std::vector<bool> attributionResultsLastTouch1D{
      /* conv 50 */ false,
      false,
      false,
      /* conv 150 */ false,
      true,
      false,
      /* conv 87000 */ false,
      false,
      false};

  auto lastClick1D = AttributionRule<
      common::PUBLISHER,
      false,
      common::InputEncryption::Plaintext>::
      fromNameOrThrow(common::LAST_CLICK_1D);
  auto thresholdsLastClick1D =
      game.privatelyShareThresholds(
              touchpoints, privateTouchpoints, *lastClick1D, 0)
          .at(0);
  auto lastTouch1D = AttributionRule<
      common::PUBLISHER,
      false,
      common::InputEncryption::Plaintext>::
      fromNameOrThrow(common::LAST_TOUCH_1D);
  auto thresholdsLastTouch1D =
      game.privatelyShareThresholds(
              touchpoints, privateTouchpoints, *lastTouch1D, 0)
          .at(0);

  auto computeAttributionLastClick1D = game.computeAttributionsHelper(
      privateTouchpoints.at(0),
      privateConversions.at(0),
      *lastClick1D,
      thresholdsLastClick1D,
      1);

  auto computeAttributionLastTouch1D = game.computeAttributionsHelper(
      privateTouchpoints.at(0),
      privateConversions.at(0),
      *lastTouch1D,
      thresholdsLastTouch1D,
      1);

  for (size_t i = 0; i < attributionResultsLastClick1D.size(); ++i) {
    EXPECT_EQ(
        computeAttributionLastClick1D.at(i)
            .openToParty(common::PUBLISHER)
            .getValue(),
        attributionResultsLastClick1D.at(i));
  }

  for (size_t i = 0; i < attributionResultsLastTouch1D.size(); ++i) {
    EXPECT_EQ(
        computeAttributionLastTouch1D.at(i)
            .openToParty(common::PUBLISHER)
            .getValue(),
        attributionResultsLastTouch1D.at(i));
  }
}

TEST(AttributionGameTest, TestAttributionReformattedOutputLogicPlaintext) {
  std::vector<std::vector<Touchpoint<false>>> touchpoints{
      std::vector<Touchpoint<false>>{
          Touchpoint<false>{.id = 0, .isClick = false, .ts = 125, .adId = 1},
          Touchpoint<false>{.id = 1, .isClick = true, .ts = 100, .adId = 2},
          Touchpoint<false>{.id = 2, .isClick = true, .ts = 200, .adId = 3}}};

  std::vector<std::vector<Conversion<false>>> conversions{
      std::vector<Conversion<false>>{
          Conversion<false>{.ts = 50, .convValue = 20},
          Conversion<false>{.ts = 150, .convValue = 40},
          Conversion<false>{.ts = 87000, .convValue = 60}}};

  AttributionGame<common::PUBLISHER, false, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto privateTouchpoints = game.privatelyShareTouchpoints(touchpoints);
  auto privateConversions = game.privatelyShareConversions(conversions);

  std::vector<bool> attributionResultsLastClick1D{false, true, false};
  std::vector<int> adIdsLastClick1D{0, 2, 0};
  std::vector<int> convValuesLastClick1D{20, 40, 60};

  std::vector<bool> attributionResultsLastTouch1D{false, true, false};
  std::vector<int> adIdsLastTouch1D{0, 2, 0};
  std::vector<int> convValuesLastTouch1D{20, 40, 60};

  auto lastClick1D = AttributionRule<
      common::PUBLISHER,
      false,
      common::InputEncryption::Plaintext>::
      fromNameOrThrow(common::LAST_CLICK_1D);
  auto thresholdsLastClick1D =
      game.privatelyShareThresholds(
              touchpoints, privateTouchpoints, *lastClick1D, 0)
          .at(0);
  auto lastTouch1D = AttributionRule<
      common::PUBLISHER,
      false,
      common::InputEncryption::Plaintext>::
      fromNameOrThrow(common::LAST_TOUCH_1D);
  auto thresholdsLastTouch1D =
      game.privatelyShareThresholds(
              touchpoints, privateTouchpoints, *lastTouch1D, 0)
          .at(0);

  auto computeAttributionLastClick1DReformattedOutputFormat =
      game.computeAttributionsHelperV2(
          privateTouchpoints.at(0),
          privateConversions.at(0),
          *lastClick1D,
          thresholdsLastClick1D,
          1);

  auto computeAttributionLastTouch1DReformattedOutputFormat =
      game.computeAttributionsHelperV2(
          privateTouchpoints.at(0),
          privateConversions.at(0),
          *lastTouch1D,
          thresholdsLastTouch1D,
          1);

  for (size_t i = 0; i < attributionResultsLastClick1D.size(); ++i) {
    EXPECT_EQ(
        computeAttributionLastClick1DReformattedOutputFormat.at(i)
            .is_attributed.openToParty(common::PUBLISHER)
            .getValue(),
        attributionResultsLastClick1D.at(i));
    EXPECT_EQ(
        computeAttributionLastClick1DReformattedOutputFormat.at(i)
            .ad_id.openToParty(common::PUBLISHER)
            .getValue(),
        adIdsLastClick1D.at(i));
    EXPECT_EQ(
        computeAttributionLastClick1DReformattedOutputFormat.at(i)
            .conv_value.openToParty(common::PUBLISHER)
            .getValue(),
        convValuesLastClick1D.at(i));
  }

  for (size_t i = 0; i < attributionResultsLastTouch1D.size(); ++i) {
    EXPECT_EQ(
        computeAttributionLastTouch1DReformattedOutputFormat.at(i)
            .is_attributed.openToParty(common::PUBLISHER)
            .getValue(),
        attributionResultsLastTouch1D.at(i));
    EXPECT_EQ(
        computeAttributionLastTouch1DReformattedOutputFormat.at(i)
            .ad_id.openToParty(common::PUBLISHER)
            .getValue(),
        adIdsLastTouch1D.at(i));
    EXPECT_EQ(
        computeAttributionLastTouch1DReformattedOutputFormat.at(i)
            .conv_value.openToParty(common::PUBLISHER)
            .getValue(),
        convValuesLastTouch1D.at(i));
  }
}

TEST(AttributionGameTest, TestAttributionLogicPlaintextBatch) {
  int batchSize = 2;

  std::vector<Touchpoint<true>> touchpoints{
      Touchpoint<true>{{0, 0}, {false, false}, {125, 125}},
      Touchpoint<true>{{1, 1}, {true, true}, {100, 100}},
      Touchpoint<true>{{2, 2}, {true, true}, {200, 200}}};

  std::vector<Conversion<true>> conversions{
      Conversion<true>{{50, 50}},
      Conversion<true>{{150, 150}},
      Conversion<true>{{87000, 87000}}};

  AttributionGame<common::PUBLISHER, true, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto privateTouchpoints = game.privatelyShareTouchpoints(touchpoints);
  auto privateConversions = game.privatelyShareConversions(conversions);

  std::vector<bool> attributionResultsLastClick1D{
      /* conv 50 */ false,
      false,
      false,
      /* conv 150 */ false,
      true,
      false,
      /* conv 87000 */ false,
      false,
      false};

  std::vector<bool> attributionResultsLastTouch1D{
      /* conv 50 */ false,
      false,
      false,
      /* conv 150 */ false,
      true,
      false,
      /* conv 87000 */ false,
      false,
      false};

  auto lastClick1D = AttributionRule<
      common::PUBLISHER,
      true,
      common::InputEncryption::Plaintext>::
      fromNameOrThrow(common::LAST_CLICK_1D);
  auto lastTouch1D = AttributionRule<
      common::PUBLISHER,
      true,
      common::InputEncryption::Plaintext>::
      fromNameOrThrow(common::LAST_TOUCH_1D);
  auto thresholdsLastClick1D = game.privatelyShareThresholds(
      touchpoints, privateTouchpoints, *lastClick1D, 2);
  auto thresholdsLastTouch1D = game.privatelyShareThresholds(
      touchpoints, privateTouchpoints, *lastTouch1D, 2);

  auto computeAttributionLastClick1D = game.computeAttributionsHelper(
      privateTouchpoints,
      privateConversions,
      *lastClick1D,
      thresholdsLastClick1D,
      batchSize);

  auto computeAttributionLastTouch1D = game.computeAttributionsHelper(
      privateTouchpoints,
      privateConversions,
      *lastTouch1D,
      thresholdsLastTouch1D,
      batchSize);

  for (size_t i = 0; i < attributionResultsLastClick1D.size(); ++i) {
    for (size_t j = 0; j < batchSize; ++j) {
      EXPECT_EQ(
          computeAttributionLastClick1D.at(i)
              .openToParty(common::PUBLISHER)
              .getValue()
              .at(j),
          attributionResultsLastClick1D.at(i));
    }
  }

  for (size_t i = 0; i < attributionResultsLastTouch1D.size(); ++i) {
    for (size_t j = 0; j < batchSize; ++j) {
      EXPECT_EQ(
          computeAttributionLastTouch1D.at(i)
              .openToParty(common::PUBLISHER)
              .getValue()
              .at(j),
          attributionResultsLastTouch1D.at(i));
    }
  }
}

TEST(AttributionGameTest, TestAttributionReformattedOutputLogicPlaintextBatch) {
  int batchSize = 2;

  std::vector<Touchpoint<true>> touchpoints{
      Touchpoint<true>{
          .id = {0, 0},
          .isClick = {false, false},
          .ts = {125, 125},
          .adId = {1, 1}},
      Touchpoint<true>{
          .id = {1, 1},
          .isClick = {true, true},
          .ts = {100, 100},
          .adId = {2, 2}},
      Touchpoint<true>{
          .id = {2, 2},
          .isClick = {true, true},
          .ts = {200, 200},
          .adId = {3, 3}}};

  std::vector<Conversion<true>> conversions{
      Conversion<true>{.ts = {50, 50}, .convValue = {20, 20}},
      Conversion<true>{.ts = {150, 150}, .convValue = {40, 40}},
      Conversion<true>{.ts = {87000, 87000}, .convValue = {60, 60}}};

  AttributionGame<common::PUBLISHER, true, common::InputEncryption::Plaintext>
      game(std::make_unique<fbpcf::scheduler::PlaintextScheduler>(
          fbpcf::scheduler::WireKeeper::createWithVectorArena<unsafe>()));

  auto privateTouchpoints = game.privatelyShareTouchpoints(touchpoints);
  auto privateConversions = game.privatelyShareConversions(conversions);

  std::vector<std::vector<bool>> attributionResultsLastClick1D{
      {false, false}, {true, true}, {false, false}};
  std::vector<std::vector<int>> adIdsLastClick1D{{0, 0}, {2, 2}, {0, 0}};
  std::vector<std::vector<int>> convValuesLastClick1D{
      {20, 20}, {40, 40}, {60, 60}};

  std::vector<std::vector<bool>> attributionResultsLastTouch1D{
      {false, false}, {true, true}, {false, false}};
  std::vector<std::vector<int>> adIdsLastTouch1D{{0, 0}, {2, 2}, {0, 0}};
  std::vector<std::vector<int>> convValuesLastTouch1D{
      {20, 20}, {40, 40}, {60, 60}};

  auto lastClick1D = AttributionRule<
      common::PUBLISHER,
      true,
      common::InputEncryption::Plaintext>::
      fromNameOrThrow(common::LAST_CLICK_1D);
  auto lastTouch1D = AttributionRule<
      common::PUBLISHER,
      true,
      common::InputEncryption::Plaintext>::
      fromNameOrThrow(common::LAST_TOUCH_1D);
  auto thresholdsLastClick1D = game.privatelyShareThresholds(
      touchpoints, privateTouchpoints, *lastClick1D, 2);
  auto thresholdsLastTouch1D = game.privatelyShareThresholds(
      touchpoints, privateTouchpoints, *lastTouch1D, 2);

  auto computeAttributionLastClick1DReformattedOutputFormat =
      game.computeAttributionsHelperV2(
          privateTouchpoints,
          privateConversions,
          *lastClick1D,
          thresholdsLastClick1D,
          batchSize);

  auto computeAttributionLastTouch1DReformattedOutputFormat =
      game.computeAttributionsHelperV2(
          privateTouchpoints,
          privateConversions,
          *lastTouch1D,
          thresholdsLastTouch1D,
          batchSize);

  for (size_t i = 0; i < attributionResultsLastClick1D.size(); ++i) {
    for (size_t j = 0; j < batchSize; ++j) {
      EXPECT_EQ(
          computeAttributionLastClick1DReformattedOutputFormat.at(i)
              .is_attributed.openToParty(common::PUBLISHER)
              .getValue()
              .at(j),
          attributionResultsLastClick1D.at(i).at(j));
      EXPECT_EQ(
          computeAttributionLastClick1DReformattedOutputFormat.at(i)
              .ad_id.openToParty(common::PUBLISHER)
              .getValue()
              .at(j),
          adIdsLastClick1D.at(i).at(j));
      EXPECT_EQ(
          computeAttributionLastClick1DReformattedOutputFormat.at(i)
              .conv_value.openToParty(common::PUBLISHER)
              .getValue()
              .at(j),
          convValuesLastClick1D.at(i).at(j));
    }
  }

  for (size_t i = 0; i < attributionResultsLastTouch1D.size(); ++i) {
    for (size_t j = 0; j < batchSize; ++j) {
      EXPECT_EQ(
          computeAttributionLastTouch1DReformattedOutputFormat.at(i)
              .is_attributed.openToParty(common::PUBLISHER)
              .getValue()
              .at(j),
          attributionResultsLastTouch1D.at(i).at(j));
      EXPECT_EQ(
          computeAttributionLastTouch1DReformattedOutputFormat.at(i)
              .ad_id.openToParty(common::PUBLISHER)
              .getValue()
              .at(j),
          adIdsLastTouch1D.at(i).at(j));
      EXPECT_EQ(
          computeAttributionLastTouch1DReformattedOutputFormat.at(i)
              .conv_value.openToParty(common::PUBLISHER)
              .getValue()
              .at(j),
          convValuesLastTouch1D.at(i).at(j));
    }
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
AttributionOutputMetrics computeAttributionsWithScheduler(
    int myId,
    AttributionInputMetrics<usingBatch, inputEncryption> inputData,
    std::reference_wrapper<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    fbpcf::SchedulerCreator schedulerCreator) {
  auto scheduler = schedulerCreator(myId, factory);
  auto game = std::make_unique<
      AttributionGame<schedulerId, usingBatch, inputEncryption>>(
      std::move(scheduler));
  return game->computeAttributions(myId, inputData);
}

template <bool usingBatch, common::InputEncryption inputEncryption>
void testCorrectnessWithScheduler(
    string attributionRule,
    fbpcf::SchedulerCreator schedulerCreator,
    bool useNewOutputFormat) {
  std::string baseDir_ =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);
  std::string filePrefix = baseDir_ + "test_correctness/" + attributionRule;
  std::string outputJsonFileName = filePrefix + ".json";

  std::string reformattedOutputJsonFileName =
      baseDir_ + "test_correctness/" + attributionRule + "_reformatted.json";

  if constexpr (inputEncryption == common::InputEncryption::PartnerXor) {
    filePrefix = filePrefix + ".partner_xor";
  } else if constexpr (inputEncryption == common::InputEncryption::Xor) {
    filePrefix = filePrefix + ".xor";
  }
  std::string publisherInputFileName = filePrefix + ".publisher.csv";
  std::string partnerInputFileName = filePrefix + ".partner.csv";

  // read input files
  AttributionInputMetrics<usingBatch, inputEncryption> publisherInputData{
      common::PUBLISHER, attributionRule, publisherInputFileName};
  AttributionInputMetrics<usingBatch, inputEncryption> partnerInputData{
      common::PARTNER, attributionRule, partnerInputFileName};

  // compute attributions
  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

  FLAGS_use_new_output_format = useNewOutputFormat;

  auto future0 = std::async(
      computeAttributionsWithScheduler<0, usingBatch, inputEncryption>,
      0,
      publisherInputData,
      std::reference_wrapper<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>(
          *factories[0]),
      schedulerCreator);
  auto future1 = std::async(
      computeAttributionsWithScheduler<1, usingBatch, inputEncryption>,
      1,
      partnerInputData,
      std::reference_wrapper<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>(
          *factories[1]),
      schedulerCreator);

  auto res0 = future0.get();
  auto res1 = future1.get();

  if (useNewOutputFormat) {
    // check against expected output for reformatted output
    auto outputReformatted =
        revealXORedReformattedResult(res0, res1, attributionRule);
    verifyOutput(outputReformatted, reformattedOutputJsonFileName);
  } else {
    // check against expected output
    auto output = revealXORedResult(res0, res1, attributionRule);
    verifyOutput(output, outputJsonFileName);
  }
}

template <bool usingBatch, common::InputEncryption inputEncryption>
void testInputColumnsWithScheduler(
    string attributionRule,
    fbpcf::SchedulerCreator schedulerCreator,
    string columnName,
    bool useNewOutputFormat) {
  std::string baseDir_ =
      private_measurement::test_util::getBaseDirFromPath(__FILE__);
  std::string filePrefix = baseDir_ + "test_correctness/" + attributionRule;
  std::string outputJsonFileName = filePrefix + ".json";

  std::string reformattedOutputJsonFileName =
      baseDir_ + "test_correctness/" + attributionRule + "_reformatted.json";

  filePrefix = filePrefix + "." + columnName;
  std::string publisherInputFileName = filePrefix + ".publisher.csv";
  std::string partnerInputFileName = filePrefix + ".partner.csv";

  // read input files
  AttributionInputMetrics<usingBatch, inputEncryption> publisherInputData{
      common::PUBLISHER, attributionRule, publisherInputFileName};
  AttributionInputMetrics<usingBatch, inputEncryption> partnerInputData{
      common::PARTNER, attributionRule, partnerInputFileName};

  // compute attributions
  auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

  FLAGS_use_new_output_format = useNewOutputFormat;

  auto future0 = std::async(
      computeAttributionsWithScheduler<0, usingBatch, inputEncryption>,
      0,
      publisherInputData,
      std::reference_wrapper<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>(
          *factories[0]),
      schedulerCreator);

  auto future1 = std::async(
      computeAttributionsWithScheduler<1, usingBatch, inputEncryption>,
      1,
      partnerInputData,
      std::reference_wrapper<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>(
          *factories[1]),
      schedulerCreator);

  auto res0 = future0.get();
  auto res1 = future1.get();

  if (useNewOutputFormat) {
    // check against expected reformatted output
    auto outputReformatted =
        revealXORedReformattedResult(res0, res1, attributionRule);
    verifyOutput(outputReformatted, reformattedOutputJsonFileName);
  } else {
    // check against expected output
    auto output = revealXORedResult(res0, res1, attributionRule);
    verifyOutput(output, outputJsonFileName);
  }
}

class AttributionGameTestFixture : public ::testing::TestWithParam<std::tuple<
                                       common::SchedulerType,
                                       bool,
                                       common::InputEncryption,
                                       string,
                                       bool>> {};

TEST_P(AttributionGameTestFixture, TestCorrectness) {
  auto
      [schedulerType,
       usingBatch,
       inputEncryption,
       attributionRule,
       useNewOutputFormat] = GetParam();

  fbpcf::SchedulerCreator schedulerCreator =
      fbpcf::getSchedulerCreator<unsafe>(schedulerType);

  if (usingBatch) {
    switch (inputEncryption) {
      case common::InputEncryption::Plaintext:
        testCorrectnessWithScheduler<true, common::InputEncryption::Plaintext>(
            attributionRule, schedulerCreator, useNewOutputFormat);
        break;

      case common::InputEncryption::PartnerXor:
        testCorrectnessWithScheduler<true, common::InputEncryption::PartnerXor>(
            attributionRule, schedulerCreator, useNewOutputFormat);
        break;

      case common::InputEncryption::Xor:
        testCorrectnessWithScheduler<true, common::InputEncryption::Xor>(
            attributionRule, schedulerCreator, useNewOutputFormat);
        break;
    }
  } else {
    switch (inputEncryption) {
      case common::InputEncryption::Plaintext:
        testCorrectnessWithScheduler<false, common::InputEncryption::Plaintext>(
            attributionRule, schedulerCreator, useNewOutputFormat);
        break;

      case common::InputEncryption::PartnerXor:
        testCorrectnessWithScheduler<
            false,
            common::InputEncryption::PartnerXor>(
            attributionRule, schedulerCreator, useNewOutputFormat);
        break;

      case common::InputEncryption::Xor:
        testCorrectnessWithScheduler<false, common::InputEncryption::Xor>(
            attributionRule, schedulerCreator, useNewOutputFormat);
        break;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    AttributionGameTest,
    AttributionGameTestFixture,
    ::testing::Combine(
        ::testing::Values(
            common::SchedulerType::NetworkPlaintext,
            common::SchedulerType::Eager,
            common::SchedulerType::Lazy),
        ::testing::Bool(),
        ::testing::Values(
            common::InputEncryption::Plaintext,
            common::InputEncryption::PartnerXor,
            common::InputEncryption::Xor),
        ::testing::Values(
            common::LAST_CLICK_1D,
            common::LAST_TOUCH_1D,
            common::LAST_CLICK_2_7D,
            common::LAST_TOUCH_2_7D,
            common::LAST_CLICK_1D_TARGETID),
        ::testing::Bool()),

    [](const testing::TestParamInfo<AttributionGameTestFixture::ParamType>&
           info) {
      auto schedulerType = std::get<0>(info.param);
      auto batch = std::get<1>(info.param) ? "Batch" : "";
      auto inputEncryption = std::get<2>(info.param);
      auto attributionRule = std::get<3>(info.param);
      auto newOutputFormat = std::get<4>(info.param) ? "NewOutputFormat" : "";

      return getSchedulerName(schedulerType) + batch +
          getInputEncryptionString(inputEncryption) + "_" + attributionRule +
          "_" + newOutputFormat;
    });

class AttributionGameInputTestFixture
    : public ::testing::TestWithParam<std::tuple<
          common::SchedulerType,
          common::InputEncryption,
          string,
          string,
          bool>> {};

TEST_P(AttributionGameInputTestFixture, TestCorrectness) {
  auto
      [schedulerType,
       inputEncryption,
       attributionRule,
       inputColumn,
       useNewOutputFormat] = GetParam();

  fbpcf::SchedulerCreator schedulerCreator =
      fbpcf::getSchedulerCreator<unsafe>(schedulerType);

  testInputColumnsWithScheduler<true, common::InputEncryption::Plaintext>(
      attributionRule, schedulerCreator, inputColumn, useNewOutputFormat);
}

INSTANTIATE_TEST_SUITE_P(
    AttributionGameTest,
    AttributionGameInputTestFixture,
    ::testing::Combine(
        ::testing::Values(
            common::SchedulerType::NetworkPlaintext,
            common::SchedulerType::Eager,
            common::SchedulerType::Lazy),
        ::testing::Values(common::InputEncryption::Plaintext),
        ::testing::Values(common::LAST_CLICK_1D),
        ::testing::Values(common::TARGET_ID, common::TARGET_ID_ACTION_TYPE),
        ::testing::Bool()),
    [](const testing::TestParamInfo<AttributionGameInputTestFixture::ParamType>&
           info) {
      auto schedulerType = std::get<0>(info.param);
      auto inputEncryption = std::get<1>(info.param);
      auto attributionRule = std::get<2>(info.param);
      auto inputColumn = std::get<3>(info.param);
      auto newOutputFormat = std::get<4>(info.param) ? "NewOutputFormat" : "";

      return fbpcf::getSchedulerName(schedulerType) +
          getInputEncryptionString(inputEncryption) + "_" + attributionRule +
          "_" + inputColumn + "_" + newOutputFormat;
      return inputColumn;
    });

} // namespace pcf2_attribution
