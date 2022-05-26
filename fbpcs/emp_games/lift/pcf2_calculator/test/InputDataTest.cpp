/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "folly/Random.h"

#include "../../../common/TestUtil.h"
#include "../InputData.h"

namespace private_lift {
class InputDataTest : public ::testing::Test {
 public:
 protected:
  std::string aliceInputFilename_;
  std::string aliceInputFilename2_;
  std::string bobInputFilename_;
  std::string bobInputFilename2_;

  void SetUp() override {
    std::string baseDir =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    aliceInputFilename_ = baseDir + "../sample_input/publisher_unittest.csv";
    aliceInputFilename2_ = baseDir + "../sample_input/publisher_unittest2.csv";
    bobInputFilename_ =
        baseDir + "../sample_input/partner_4_convs_unittest.csv";
    bobInputFilename2_ =
        baseDir + "../sample_input/partner_converter_unittest.csv";
  }
};

TEST_F(InputDataTest, TestInputDataPublisher) {
  InputData inputData{
      aliceInputFilename_, InputData::LiftMPCType::Standard, 1546300800, 4};
  std::vector<bool> expectTestPopulation = {0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
                                            0, 0, 0, 1, 1, 0, 0, 1, 0, 0};
  std::vector<bool> expectControlPopulation = {1, 0, 0, 0, 0, 0, 1, 1, 1, 1,
                                               0, 0, 1, 0, 0, 1, 1, 0, 1, 1};
  // opportunity_timestamp - epoch
  std::vector<uint32_t> expectOpportunityTimestamps = {
      53699630, 53699601, 0,        0,        0,        53699661, 53699252,
      53700031, 53699730, 53700172, 0,        0,        53699306, 53700140,
      53699240, 53699397, 53699415, 53700127, 53699760, 53699598};
  auto resTestPopulation = inputData.getTestPopulation();
  auto resControlPopulation = inputData.getControlPopulation();
  auto resOpportunityTimestamps = inputData.getOpportunityTimestamps();
  EXPECT_EQ(expectTestPopulation, resTestPopulation);
  EXPECT_EQ(expectControlPopulation, resControlPopulation);
  EXPECT_EQ(expectOpportunityTimestamps, resOpportunityTimestamps);
}

TEST_F(InputDataTest, TestInputDataPublisherOppColLast) {
  InputData inputData{
      aliceInputFilename2_, InputData::LiftMPCType::Standard, 1546300800, 4};
  std::vector<bool> expectTestPopulation = {0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
                                            0, 0, 0, 1, 1, 0, 0, 1, 0, 0};
  std::vector<bool> expectControlPopulation = {1, 0, 0, 0, 0, 0, 1, 1, 1, 1,
                                               0, 0, 1, 0, 0, 1, 1, 0, 1, 1};
  // opportunity_timestamp - epoch
  std::vector<uint32_t> expectOpportunityTimestamps = {
      53699630, 53699601, 0,        0,        0,        53699661, 53699252,
      53700031, 53699730, 53700172, 0,        0,        53699306, 53700140,
      53699240, 53699397, 53699415, 53700127, 53699760, 53699598};
  auto resTestPopulation = inputData.getTestPopulation();
  auto resControlPopulation = inputData.getControlPopulation();
  auto resOpportunityTimestamps = inputData.getOpportunityTimestamps();
  EXPECT_EQ(expectTestPopulation, resTestPopulation);
  EXPECT_EQ(expectControlPopulation, resControlPopulation);
  EXPECT_EQ(expectOpportunityTimestamps, resOpportunityTimestamps);
}

TEST_F(InputDataTest, TestInputDataPartner) {
  InputData inputData{
      bobInputFilename_,
      InputData::LiftMPCType::Standard,
      1546300800, /* epoch */
      4 /* num_conversions_per_user */};
  std::vector<std::vector<uint32_t>> expectGetPurchaseTimestampArrays = {
      {0, 0, 0, 0},
      {0, 0, 53699530, 53699794},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 53699428},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 53699222, 53699836, 53699923},
      {53699839, 53699868, 53700039, 53700058},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0},
      {0, 0, 0, 0}};
  std::vector<std::vector<int64_t>> expectPurchaseValueArrays = {
      {0, 0, 0, 0},  {0, 0, 71, 71}, {0, 0, 0, 0},    {0, 0, 0, 0},
      {0, 0, 0, 25}, {0, 0, 0, 0},   {0, 0, 0, 0},    {0, 0, 0, 0},
      {0, 0, 0, 0},  {0, 0, 0, 0},   {0, 0, 0, 0},    {0, 0, 0, 0},
      {0, 0, 0, 0},  {0, 0, 0, 0},   {0, 47, 57, 51}, {63, 69, 21, 24},
      {0, 0, 0, 0},  {0, 0, 0, 0},   {0, 0, 0, 0},    {0, 0, 0, 0},
  };
  std::vector<uint32_t> expectCohortIds = {0, 1, 0, 0, 2, 0, 0, 0, 0, 0,
                                           0, 0, 0, 0, 1, 2, 0, 0, 0, 0};
  auto resPurchaseTimestampArrays = inputData.getPurchaseTimestampArrays();
  auto resPurchaseValueArrays = inputData.getPurchaseValueArrays();
  EXPECT_EQ(expectGetPurchaseTimestampArrays, resPurchaseTimestampArrays);
  EXPECT_EQ(expectPurchaseValueArrays, resPurchaseValueArrays);

  ASSERT_EQ(3, inputData.getNumGroups());
  EXPECT_EQ(expectCohortIds, inputData.getGroupIds());
}

TEST_F(InputDataTest, TestInputDataPartnerConverterLift) {
  InputData inputData{
      bobInputFilename2_,
      InputData::LiftMPCType::Standard,
      0, /* epoch */
      1 /* num_conversions_per_user */};
  std::vector<std::vector<uint32_t>> expectGetPurchaseTimestamps = {
      {0},          {1600000594}, {0}, {0}, {1600000228}, {0}, {0},
      {0},          {0},          {0}, {0}, {0},          {0}, {0},
      {1600000723}, {1600000858}, {0}, {0}, {0},          {0}};
  std::vector<int64_t> expectPurchaseValues = {0, 71, 0, 0, 25, 0,  0, 0, 0, 0,
                                               0, 0,  0, 0, 51, 24, 0, 0, 0, 0};
  std::vector<int64_t> expectPurchaseValuesSquared = {
      0, 71 * 71, 0, 0, 25 * 25, 0,       0, 0, 0, 0,
      0, 0,       0, 0, 51 * 51, 24 * 24, 0, 0, 0, 0};
  auto resPurchaseTimestamps = inputData.getPurchaseTimestampArrays();
  auto resPurchaseValues = inputData.getPurchaseValues();
  auto resPurchaseValuesSquared = inputData.getPurchaseValuesSquared();
  EXPECT_EQ(expectGetPurchaseTimestamps, resPurchaseTimestamps);
  EXPECT_EQ(expectPurchaseValues, resPurchaseValues);
  EXPECT_EQ(expectPurchaseValuesSquared, resPurchaseValuesSquared);
}

TEST_F(InputDataTest, TestGetBitmaskFor) {
  InputData inputData{
      bobInputFilename_,
      InputData::LiftMPCType::Standard,
      1546300800, /* epoch */
      4 /* num_conversions_per_user */};

  std::vector<int64_t> expectCohortIds = {0, 1, 0, 0, 2, 0, 0, 0, 0, 0,
                                          0, 0, 0, 0, 1, 2, 0, 0, 0, 0};
  std::vector<int64_t> bitmask0 = {1, 0, 1, 1, 0, 1, 1, 1, 1, 1,
                                   1, 1, 1, 1, 0, 0, 1, 1, 1, 1};
  std::vector<int64_t> bitmask1 = {0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
                                   0, 0, 0, 0, 1, 0, 0, 0, 0, 0};
  std::vector<int64_t> bitmask2 = {0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
                                   0, 0, 0, 0, 0, 1, 0, 0, 0, 0};
  EXPECT_EQ(bitmask0, inputData.bitmaskFor(0));
  EXPECT_EQ(bitmask1, inputData.bitmaskFor(1));
  EXPECT_EQ(bitmask2, inputData.bitmaskFor(2));
}

} // namespace private_lift
