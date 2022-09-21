/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <vector>
#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/Constants.h"

#include "folly/logging/xlog.h"

namespace private_lift {

inline const std::vector<std::string> GLOBAL_PARAMS_HEADER = {
    "numPartnerCohorts",
    "numPublisherBreakdowns",
    "numGroups",
    "numTestGroups",
    "valueBits",
    "valueSquaredBits",
};

inline const std::vector<std::string> SECRET_SHARES_HEADER = {
    "id_",
    "indexShares",
    "testIndexShares",
    "opportunityTimestamps",
    "isValidOpportunityTimestamp",
    "purchaseTimestamps",
    "thresholdTimestamps",
    "anyValidPurchaseTimestamp",
    "purchaseValues",
    "purchaseValueSquared",
    "testReach"};

template <int schedulerId>
struct LiftGameProcessedData {
  int64_t numRows;
  uint32_t numPartnerCohorts;
  uint32_t numPublisherBreakdowns;
  uint32_t numGroups;
  uint32_t numTestGroups;
  uint8_t valueBits;
  uint8_t valueSquaredBits;
  std::vector<std::vector<bool>> indexShares;
  std::vector<std::vector<bool>> testIndexShares;
  SecTimestamp<schedulerId> opportunityTimestamps;
  SecBit<schedulerId> isValidOpportunityTimestamp;
  std::vector<SecTimestamp<schedulerId>> purchaseTimestamps;
  std::vector<SecTimestamp<schedulerId>> thresholdTimestamps;
  SecBit<schedulerId> anyValidPurchaseTimestamp;
  std::vector<SecValue<schedulerId>> purchaseValues;
  std::vector<SecValueSquared<schedulerId>> purchaseValueSquared;
  SecBit<schedulerId> testReach;

  void writeToCSV(
      const std::string& globalParamsOutputPath,
      const std::string& secretSharesOutputPath) const;

  static LiftGameProcessedData readFromCSV(
      const std::string& globalParamsInputPath,
      const std::string& secretSharesInputPath);

 private:
  template <typename T>
  static std::string joinColumn(
      const std::vector<std::vector<T>>& data,
      size_t columnIndex);

  template <typename T>
  static std::vector<T> extractColumn(
      const std::vector<std::vector<T>>& data,
      size_t columnIndex);

  template <typename T>
  static std::vector<std::vector<T>> transpose(
      const std::vector<std::vector<T>>& data);

  static std::vector<std::string> splitValueArray(const std::string& str);

  static std::function<
      void(const std::vector<std::string>&, const std::vector<std::string>&)>
  readParamsLine(LiftGameProcessedData<schedulerId>& result);

  static std::function<
      void(const std::vector<std::string>&, const std::vector<std::string>&)>
  readSharesLine(
      LiftGameProcessedData<schedulerId>& result,
      std::vector<std::vector<bool>>& indexShares,
      std::vector<std::vector<bool>>& testIndexShares,
      std::vector<uint64_t>& opportunityTimestampsShares,
      std::vector<bool>& isValidOpportunityTimestampShares,
      std::vector<std::vector<uint64_t>>& purchaseTimestampShares,
      std::vector<std::vector<uint64_t>>& thresholdTimestampShares,
      std::vector<bool>& anyValidPurchaseTimestampShares,
      std::vector<std::vector<int64_t>>& purchaseValueShares,
      std::vector<std::vector<int64_t>>& purchaseValueSquaredShares,
      std::vector<bool>& testReachShares);
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/LiftGameProcessedData_impl.h"
