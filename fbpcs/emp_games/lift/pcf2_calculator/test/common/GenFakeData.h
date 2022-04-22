/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "fbpcs/emp_games/lift/pcf2_calculator/InputData.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/LiftFakeDataParams.h"

namespace private_lift {
class GenFakeData {
 public:
  void genFakePublisherInputFile(
      std::string filename,
      const LiftFakeDataParams& params);
  void genFakePartnerInputFile(
      std::string filename,
      const LiftFakeDataParams& params);

 private:
  struct LiftInputColumns {
    // publisher header:
    //   id_,opportunity,test_flag,opportunity_timestamp,num_impressions,num_clicks,total_spend

    // partner header: id_,event_timestamps,values
    std::string id;
    bool opportunity;
    bool test_flag;
    int32_t opportunity_timestamp;
    int32_t num_impressions;
    int32_t num_clicks;
    int32_t total_spend;
    std::vector<int32_t> event_timestamps;
    std::vector<int32_t> values;
  };
  LiftInputColumns genOneFakeLine(
      const std::string& id,
      double opportunityRate,
      double testRate,
      double purchaseRate,
      double incrementalityRate,
      int32_t epoch,
      int32_t numConversions);
  double genAdjustedPurchaseRate(
      bool isTest,
      double purchaseRate,
      double incrementalityRate);
};

} // namespace private_lift
