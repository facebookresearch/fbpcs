/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
  std::string bobInputFilename_;
  void SetUp() override {
    std::string baseDir =
        private_measurement::test_util::getBaseDirFromPath(__FILE__);
    aliceInputFilename_ = baseDir + "../sample_input/publisher_unittest.csv";
    bobInputFilename_ =
        baseDir + "../sample_input/partner_4_convs_unittest.csv";
  }
};

TEST_F(InputDataTest, TestInputDataPublisher) {
  InputData inputData{
      aliceInputFilename_,
      InputData::LiftMPCType::Standard,
      InputData::LiftGranularityType::Conversion,
      1546300800,
      4};
  std::vector<int64_t> expectTestPopulation = {0, 1, 0, 0, 0, 1, 0, 0, 0, 0,
                                               0, 0, 0, 1, 1, 0, 0, 1, 0, 0};
  std::vector<int64_t> expectControlPopulation = {1, 0, 0, 0, 0, 0, 1, 1, 1, 1,
                                                  0, 0, 1, 0, 0, 1, 1, 0, 1, 1};
  // opportunity_timestamp - epoch
  std::vector<int64_t> expectOpportunityTimestamps = {
      53699630,    53699601,    -1546300800, -1546300800, -1546300800,
      53699661,    53699252,    53700031,    53699730,    53700172,
      -1546300800, -1546300800, 53699306,    53700140,    53699240,
      53699397,    53699415,    53700127,    53699760,    53699598};
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
      InputData::LiftGranularityType::Conversion,
      1546300800, /* epoch */
      4 /* num_conversions_per_user */};
  std::vector<std::vector<int64_t>> expectgetPurchaseTimestampArrays = {
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, 53699530, 53699794},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, 53699428},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, 53699222, 53699836, 53699923},
      {53699839, 53699868, 53700039, 53700058},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800},
      {-1546300800, -1546300800, -1546300800, -1546300800}};
  std::vector<std::vector<int64_t>> expectPurchaseValueArrays = {
      {0, 0, 0, 0},  {0, 0, 71, 71}, {0, 0, 0, 0},    {0, 0, 0, 0},
      {0, 0, 0, 25}, {0, 0, 0, 0},   {0, 0, 0, 0},    {0, 0, 0, 0},
      {0, 0, 0, 0},  {0, 0, 0, 0},   {0, 0, 0, 0},    {0, 0, 0, 0},
      {0, 0, 0, 0},  {0, 0, 0, 0},   {0, 47, 57, 51}, {63, 69, 21, 24},
      {0, 0, 0, 0},  {0, 0, 0, 0},   {0, 0, 0, 0},    {0, 0, 0, 0},
  };
  auto resPurchaseTimestampArrays = inputData.getPurchaseTimestampArrays();
  auto resPurchaseValueArrays = inputData.getPurchaseValueArrays();
  EXPECT_EQ(expectgetPurchaseTimestampArrays, resPurchaseTimestampArrays);
  EXPECT_EQ(expectPurchaseValueArrays, resPurchaseValueArrays);
}

} // namespace private_lift
