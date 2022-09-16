/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <iterator>
#include <string>

#include "fbpcs/emp_games/lift/pcf2_calculator/LiftGameProcessedData.h"

namespace private_lift {

template <int schedulerId>
void LiftGameProcessedData<schedulerId>::writeToCSV(
    const std::string& globalParamsOutputPath,
    const std::string& secretSharesOutputPath) const {
  std::vector<std::vector<std::string>> globalParams = {
      {std::to_string(numPartnerCohorts),
       std::to_string(numPublisherBreakdowns),
       std::to_string(numGroups),
       std::to_string(numTestGroups),
       std::to_string(valueBits),
       std::to_string(valueSquaredBits)}};

  private_measurement::csv::writeCsv(
      globalParamsOutputPath, GLOBAL_PARAMS_HEADER, globalParams);

  std::vector<std::vector<std::string>> secretShares(numRows);

  std::vector<uint64_t> opportunityTimestampsShares =
      opportunityTimestamps.extractIntShare().getValue();
  std::vector<bool> isValidOpportunityTimestampShares =
      isValidOpportunityTimestamp.extractBit().getValue();
  std::vector<bool> anyValidPurchaseTimestampShares =
      anyValidPurchaseTimestamp.extractBit().getValue();
  std::vector<bool> testReachShares = testReach.extractBit().getValue();

  for (size_t i = 0; i < numRows; i++) {
    secretShares[i] = std::vector<std::string>();
    secretShares[i].reserve(SECRET_SHARES_HEADER.size());

    // id_ column
    secretShares[i].push_back(std::to_string(i));
    secretShares[i].push_back(std::to_string(opportunityTimestampsShares[i]));
    secretShares[i].push_back(
        std::to_string(isValidOpportunityTimestampShares[i]));
    secretShares[i].push_back(
        std::to_string(anyValidPurchaseTimestampShares[i]));
    secretShares[i].push_back(std::to_string(testReachShares[i]));
  }

  private_measurement::csv::writeCsv(
      secretSharesOutputPath, SECRET_SHARES_HEADER, secretShares);
}

template <int schedulerId>
LiftGameProcessedData<schedulerId>
LiftGameProcessedData<schedulerId>::readFromCSV(
    const std::string& globalParamsInputPath,
    const std::string& secretSharesInputPath) {
  LiftGameProcessedData<schedulerId> result;
  result.numRows = 0;

  private_measurement::csv::readCsv(
      globalParamsInputPath,
      [&result](
          const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        for (size_t i = 0; i < header.size(); i++) {
          auto column = header[i];
          auto value = parts[i];
          if (column == "numPartnerCohorts") {
            result.numPartnerCohorts = std::stoul(value);
          } else if (column == "numPublisherBreakdowns") {
            result.numPublisherBreakdowns = std::stoul(value);
          } else if (column == "numGroups") {
            result.numGroups = std::stoul(value);
          } else if (column == "numTestGroups") {
            result.numTestGroups = std::stoul(value);
          } else if (column == "valueBits") {
            result.valueBits = std::stoul(value);
          } else if (column == "valueSquaredBits") {
            result.valueSquaredBits = std::stoul(value);
          } else {
            LOG(WARNING) << "Warning: Unknown column in csv: " << column;
          }
        }
      });
  std::vector<uint64_t> opportunityTimestampsShares;
  std::vector<bool> isValidOpportunityTimestampShares;
  std::vector<bool> anyValidPurchaseTimestampShares;
  std::vector<bool> testReachShares;

  private_measurement::csv::readCsv(
      secretSharesInputPath,
      [&result,
       &opportunityTimestampsShares,
       &isValidOpportunityTimestampShares,
       &anyValidPurchaseTimestampShares,
       &testReachShares](
          const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        result.numRows++;
        for (size_t i = 0; i < header.size(); i++) {
          auto column = header[i];
          auto value = parts[i];
          if (column == "opportunityTimestamps") {
            opportunityTimestampsShares.push_back(std::stoull(value));
          } else if (column == "isValidOpportunityTimestamp") {
            isValidOpportunityTimestampShares.push_back(std::stoul(value));
          } else if (column == "anyValidPurchaseTimestamp") {
            anyValidPurchaseTimestampShares.push_back(std::stoul(value));
          } else if (column == "testReach") {
            testReachShares.push_back(std::stoul(value));
          } else if (column != "id_") {
            LOG(WARNING) << "Warning: Unknown column in csv: " << column;
          }
        }
      });

  if (result.numRows == 0) {
    XLOG(FATAL, "Lift Game shares file was empty");
  }

  result.opportunityTimestamps = SecTimestamp<schedulerId>(
      typename SecTimestamp<schedulerId>::ExtractedInt(
          opportunityTimestampsShares));
  result.isValidOpportunityTimestamp =
      SecBit<schedulerId>(typename SecBit<schedulerId>::ExtractedBit(
          isValidOpportunityTimestampShares));
  result.anyValidPurchaseTimestamp =
      SecBit<schedulerId>(typename SecBit<schedulerId>::ExtractedBit(
          anyValidPurchaseTimestampShares));
  result.testReach = SecBit<schedulerId>(
      typename SecBit<schedulerId>::ExtractedBit(testReachShares));

  return result;
}
} // namespace private_lift
