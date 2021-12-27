/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "common/LiftFakeDataParams.h"
#include <gtest/gtest.h>

namespace private_lift {
class LiftFakeDataParamsTest : public ::testing::Test {
 protected:
  size_t defaultNumRows_ = 10;
  double defaultOpportunityRate_ = 0.8;
  double defaultTestRate_ = 0.5;
  double defaultPurchaseRate_ = 0.3;
  double defaultIncrementalityRate_ = 0.1;
  int32_t defaultEpoch_ = 0;
  int32_t defaultNumConversions_ = 4;
  bool defaultOmitValuesColumn_ = false;
};

TEST_F(LiftFakeDataParamsTest, TestDefaultParams) {
  LiftFakeDataParams params;
  EXPECT_EQ(params.numRows_, defaultNumRows_);
  EXPECT_EQ(params.opportunityRate_, defaultOpportunityRate_);
  EXPECT_EQ(params.testRate_, defaultTestRate_);
  EXPECT_EQ(params.purchaseRate_, defaultPurchaseRate_);
  EXPECT_EQ(params.incrementalityRate_, defaultIncrementalityRate_);
  EXPECT_EQ(params.epoch_, defaultEpoch_);
  EXPECT_EQ(params.numConversions_, defaultNumConversions_);
  EXPECT_EQ(params.omitValuesColumn_, defaultOmitValuesColumn_);
}

TEST_F(LiftFakeDataParamsTest, TestSettingParams) {
  size_t numRows = 15;
  double opportunityRate = 0.5;
  double testRate = 0.5;
  double purchaseRate = 0.5;
  double incrementalityRate = 0.0;
  int32_t epoch = 1546300800;
  int32_t numConversions = 4;
  bool omitValuesColumn = true;

  LiftFakeDataParams params;
  params.setNumRows(numRows)
      .setOpportunityRate(opportunityRate)
      .setTestRate(testRate)
      .setPurchaseRate(purchaseRate)
      .setIncrementalityRate(incrementalityRate)
      .setEpoch(epoch)
      .setNumConversions(numConversions)
      .setOmitValuesColumn(omitValuesColumn);

  EXPECT_EQ(params.numRows_, numRows);
  EXPECT_EQ(params.opportunityRate_, opportunityRate);
  EXPECT_EQ(params.testRate_, testRate);
  EXPECT_EQ(params.purchaseRate_, purchaseRate);
  EXPECT_EQ(params.incrementalityRate_, incrementalityRate);
  EXPECT_EQ(params.epoch_, epoch);
  EXPECT_EQ(params.numConversions_, numConversions);
  EXPECT_EQ(params.omitValuesColumn_, omitValuesColumn);
}
} // namespace private_lift
