/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
#include <vector>
#include "fbpcf/engine/util/IPrg.h"
#include "folly/logging/xlog.h"

#include "fbpcf/mpc_std_lib/unified_data_process/adapter/IAdapter.h"
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/IDataProcessor.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/GlobalSharingUtils.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/LiftCompactionUtils.h"

namespace private_lift {
/**
 * This class handles the deserialization of serialized metadata produced from
 * UDP.
 */
template <int schedulerId>
class PostUDPInputProcessor : public IInputProcessor<schedulerId> {
 public:
  using SecString = typename fbpcf::mpc_std_lib::unified_data_process::
      data_processor::IDataProcessor<schedulerId>::SecString;

  PostUDPInputProcessor(
      int myRole,
      /* sending in shares */
      const std::vector<std::vector<bool>>& publisherMetadataShares,
      const std::vector<std::vector<bool>>& partnerMetadataShares,
      int32_t numConversionsPerUser)
      : myRole_{myRole}, numConversionsPerUser_{numConversionsPerUser} {
    liftGameProcessedData_.numRows = publisherMetadataShares.size();

    // [publisherShares, partnerShares]
    auto publisherPartnerJointMetadataShares =
        fromMemoryToMPCTypes(publisherMetadataShares, partnerMetadataShares);

    extractCompactedData(
        std::get<0>(publisherPartnerJointMetadataShares),
        std::get<1>(publisherPartnerJointMetadataShares));

    input_processing::computeIndexSharesAndSetTestGroupIds(
        liftGameProcessedData_,
        cohortGroupIds_,
        controlPopulation_,
        breakdownGroupIds_,
        testGroupIds_);
    input_processing::computeTestIndexShares(
        liftGameProcessedData_, controlPopulation_, testGroupIds_);
  }

  const LiftGameProcessedData<schedulerId>& getLiftGameProcessedData()
      const override {
    return liftGameProcessedData_;
  }

 private:
  std::pair<SecString, SecString> fromMemoryToMPCTypes(
      const std::vector<std::vector<bool>>& publisherInputShares,
      const std::vector<std::vector<bool>>& partnerInputShares);

  // deserializes the compacted data into MPC structured values
  void extractCompactedData(
      const SecString& publisherDataShares,
      const SecString& partnerDataShares);

  int32_t myRole_;

  InputData inputData_;
  int32_t numConversionsPerUser_;

  SecBit<schedulerId> controlPopulation_;
  SecGroup<schedulerId> cohortGroupIds_;
  SecBit<schedulerId> breakdownGroupIds_;
  SecGroup<schedulerId> testGroupIds_;

  LiftGameProcessedData<schedulerId> liftGameProcessedData_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/PostUDPInputProcessor_impl.h"
