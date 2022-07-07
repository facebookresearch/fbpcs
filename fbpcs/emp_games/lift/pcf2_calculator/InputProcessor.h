/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputData.h"

namespace private_lift {
/**
 * This class handles privately sharing all the input data in MPC.
 */
template <int schedulerId>
class InputProcessor {
 public:
  InputProcessor(int myRole, InputData inputData, int32_t numConversionsPerUser)
      : myRole_{myRole},
        inputData_{inputData},
        numRows_{inputData.getNumRows()},
        numConversionsPerUser_{numConversionsPerUser} {
    validateNumRowsStep();
    shareNumGroupsStep();
    shareBitsForValuesStep();
    privatelyShareGroupIdsStep();
    privatelySharePopulationStep();
    privatelyShareGroupIdsStep();
    privatelyShareIndexSharesStep();
    privatelyShareTestIndexSharesStep();
    privatelyShareTimestampsStep();
    privatelySharePurchaseValuesStep();
    privatelyShareTestReachStep();
  }

  InputProcessor() {}

  int64_t getNumRows() const {
    return numRows_;
  }

  uint32_t getNumPartnerCohorts() const {
    return numPartnerCohorts_;
  }

  uint32_t getNumPublisherBreakdowns() const {
    return numPublisherBreakdowns_;
  }

  uint32_t getNumGroups() const {
    return numGroups_;
  }

  uint32_t getNumTestGroups() const {
    return numTestGroups_;
  }

  uint8_t getValueBits() const {
    return valueBits_;
  }

  uint8_t getValueSquaredBits() const {
    return valueSquaredBits_;
  }

  const std::vector<std::vector<bool>> getIndexShares() const {
    return indexShares_;
  }

  const std::vector<std::vector<bool>> getTestIndexShares() const {
    return testIndexShares_;
  }

  const SecTimestamp<schedulerId> getOpportunityTimestamps() const {
    return opportunityTimestamps_;
  }

  const SecBit<schedulerId> getIsValidOpportunityTimestamp() const {
    return isValidOpportunityTimestamp_;
  }

  const std::vector<SecTimestamp<schedulerId>> getPurchaseTimestamps() const {
    return purchaseTimestamps_;
  }

  const std::vector<SecTimestamp<schedulerId>> getThresholdTimestamps() const {
    return thresholdTimestamps_;
  }

  const SecBit<schedulerId> getAnyValidPurchaseTimestamp() const {
    return anyValidPurchaseTimestamp_;
  }

  const std::vector<SecValue<schedulerId>> getPurchaseValues() const {
    return purchaseValues_;
  }

  const std::vector<SecValueSquared<schedulerId>> getPurchaseValueSquared()
      const {
    return purchaseValueSquared_;
  }

  const SecBit<schedulerId> getTestReach() const {
    return testReach_;
  }

 private:
  // Make sure input files have the same size
  void validateNumRowsStep();

  // Share number of groups, including cohorts and publisher breakdowns.
  void shareNumGroupsStep();

  // Share number of bits needed to store the input value and its square
  void shareBitsForValuesStep();

  // Privately share popoulation
  void privatelySharePopulationStep();

  // Privately share cohort ids and breakdown ids.
  void privatelyShareGroupIdsStep();

  // Privately share index shares of group ids encoding the population, cohorts
  // and publisher breakdowns.
  void privatelyShareIndexSharesStep();

  // Privately share index shares of group ids for the test population only.
  void privatelyShareTestIndexSharesStep();

  // Privately share timestamps
  void privatelyShareTimestampsStep();

  // Privately share purchase values and purchase values squared
  void privatelySharePurchaseValuesStep();

  // Privately share test reach (nonzero impressions)
  void privatelyShareTestReachStep();

  int32_t myRole_;
  InputData inputData_;
  int64_t numRows_;
  int32_t numConversionsPerUser_;
  uint32_t numPartnerCohorts_;
  uint32_t numPublisherBreakdowns_;
  uint32_t numGroups_;
  uint32_t numTestGroups_;
  uint8_t valueBits_;
  uint8_t valueSquaredBits_;

  SecTimestamp<schedulerId> opportunityTimestamps_;
  SecBit<schedulerId> isValidOpportunityTimestamp_;
  std::vector<SecTimestamp<schedulerId>> purchaseTimestamps_;
  std::vector<SecTimestamp<schedulerId>> thresholdTimestamps_;
  SecBit<schedulerId> anyValidPurchaseTimestamp_;
  std::vector<SecValue<schedulerId>> purchaseValues_;
  std::vector<SecValueSquared<schedulerId>> purchaseValueSquared_;
  SecBit<schedulerId> testReach_;

  SecBit<schedulerId> controlPopulation_;
  SecGroup<schedulerId> cohortGroupIds_;
  SecBit<schedulerId> breakdownGroupIds_;
  SecGroup<schedulerId> testGroupIds_;
  std::vector<std::vector<bool>> indexShares_;
  std::vector<std::vector<bool>> testIndexShares_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor_impl.h"
