/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <iterator>
#include <stdexcept>
#include <string>
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/LiftGameProcessedData.h"
#include "folly/logging/xlog.h"

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

  if (numRows == 0) {
    private_measurement::csv::writeCsv(
        secretSharesOutputPath, SECRET_SHARES_HEADER, {});
    return;
  }

  std::vector<std::vector<std::string>> secretShares(numRows);

  std::vector<uint64_t> opportunityTimestampsShares =
      opportunityTimestamps.extractIntShare().getValue();
  std::vector<bool> isValidOpportunityTimestampShares =
      isValidOpportunityTimestamp.extractBit().getValue();
  std::vector<std::vector<uint64_t>> purchaseTimestampShares;
  std::transform(
      purchaseTimestamps.begin(),
      purchaseTimestamps.end(),
      std::back_inserter(purchaseTimestampShares),
      [](const SecTimestamp<schedulerId>& purchaseTimestamp) {
        return purchaseTimestamp.extractIntShare().getValue();
      });
  std::vector<std::vector<uint64_t>> thresholdTimestampShares;
  std::transform(
      thresholdTimestamps.begin(),
      thresholdTimestamps.end(),
      std::back_inserter(thresholdTimestampShares),
      [](const SecTimestamp<schedulerId>& thresholdTimestamp) {
        return thresholdTimestamp.extractIntShare().getValue();
      });
  std::vector<bool> anyValidPurchaseTimestampShares =
      anyValidPurchaseTimestamp.extractBit().getValue();
  std::vector<std::vector<int64_t>> purchaseValueShares;
  std::transform(
      purchaseValues.begin(),
      purchaseValues.end(),
      std::back_inserter(purchaseValueShares),
      [](const SecValue<schedulerId>& purchaseValue) {
        return purchaseValue.extractIntShare().getValue();
      });
  std::vector<std::vector<int64_t>> purchaseValueSquaredShares;
  std::transform(
      purchaseValueSquared.begin(),
      purchaseValueSquared.end(),
      std::back_inserter(purchaseValueSquaredShares),
      [](const SecValueSquared<schedulerId>& purchaseValueSquared_2) {
        return purchaseValueSquared_2.extractIntShare().getValue();
      });
  std::vector<bool> testReachShares = testReach.extractBit().getValue();

  for (size_t i = 0; i < numRows; i++) {
    secretShares[i] = std::vector<std::string>();
    secretShares[i].reserve(SECRET_SHARES_HEADER.size());

    // id_ column
    secretShares[i].push_back(std::to_string(i));
    secretShares[i].push_back(joinColumn(indexShares, i));
    secretShares[i].push_back(joinColumn(testIndexShares, i));
    secretShares[i].push_back(std::to_string(opportunityTimestampsShares[i]));
    secretShares[i].push_back(
        std::to_string(isValidOpportunityTimestampShares[i]));
    secretShares[i].push_back(joinColumn(purchaseTimestampShares, i));
    secretShares[i].push_back(joinColumn(thresholdTimestampShares, i));
    secretShares[i].push_back(
        std::to_string(anyValidPurchaseTimestampShares[i]));
    secretShares[i].push_back(joinColumn(purchaseValueShares, i));
    secretShares[i].push_back(joinColumn(purchaseValueSquaredShares, i));
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
      globalParamsInputPath, readParamsLine(result));

  std::vector<std::vector<bool>> indexShares;
  std::vector<std::vector<bool>> testIndexShares;
  std::vector<uint64_t> opportunityTimestampsShares;
  std::vector<bool> isValidOpportunityTimestampShares;
  std::vector<std::vector<uint64_t>> purchaseTimestampShares;
  std::vector<std::vector<uint64_t>> thresholdTimestampShares;
  std::vector<bool> anyValidPurchaseTimestampShares;
  std::vector<std::vector<int64_t>> purchaseValueShares;
  std::vector<std::vector<int64_t>> purchaseValueSquaredShares;
  std::vector<bool> testReachShares;

  private_measurement::csv::readCsv(
      secretSharesInputPath,
      readSharesLine(
          result,
          indexShares,
          testIndexShares,
          opportunityTimestampsShares,
          isValidOpportunityTimestampShares,
          purchaseTimestampShares,
          thresholdTimestampShares,
          anyValidPurchaseTimestampShares,
          purchaseValueShares,
          purchaseValueSquaredShares,
          testReachShares));

  if (result.numRows == 0) {
    return result;
  }

  result.indexShares = transpose(indexShares);
  result.testIndexShares = transpose(testIndexShares);
  result.opportunityTimestamps = SecTimestamp<schedulerId>(
      typename SecTimestamp<schedulerId>::ExtractedInt(
          opportunityTimestampsShares));
  result.isValidOpportunityTimestamp =
      SecBit<schedulerId>(typename SecBit<schedulerId>::ExtractedBit(
          isValidOpportunityTimestampShares));

  result.purchaseTimestamps = std::vector<SecTimestamp<schedulerId>>();
  result.purchaseTimestamps.reserve(purchaseTimestampShares[0].size());
  for (size_t i = 0; i < purchaseTimestampShares[0].size(); i++) {
    result.purchaseTimestamps.push_back(SecTimestamp<schedulerId>(
        typename SecTimestamp<schedulerId>::ExtractedInt(
            extractColumn(purchaseTimestampShares, i))));
  }

  result.thresholdTimestamps = std::vector<SecTimestamp<schedulerId>>();
  result.thresholdTimestamps.reserve(thresholdTimestampShares[0].size());
  for (size_t i = 0; i < thresholdTimestampShares[0].size(); i++) {
    result.thresholdTimestamps.push_back(SecTimestamp<schedulerId>(
        typename SecTimestamp<schedulerId>::ExtractedInt(
            extractColumn(thresholdTimestampShares, i))));
  }

  result.anyValidPurchaseTimestamp =
      SecBit<schedulerId>(typename SecBit<schedulerId>::ExtractedBit(
          anyValidPurchaseTimestampShares));

  result.purchaseValues = std::vector<SecValue<schedulerId>>();
  result.purchaseValues.reserve(purchaseValueShares[0].size());
  for (size_t i = 0; i < purchaseValueShares[0].size(); i++) {
    result.purchaseValues.push_back(
        SecValue<schedulerId>(typename SecValue<schedulerId>::ExtractedInt(
            extractColumn(purchaseValueShares, i))));
  }

  result.purchaseValueSquared = std::vector<SecValueSquared<schedulerId>>();
  result.purchaseValueSquared.reserve(purchaseValueSquaredShares[0].size());
  for (size_t i = 0; i < purchaseValueSquaredShares[0].size(); i++) {
    result.purchaseValueSquared.push_back(SecValueSquared<schedulerId>(
        typename SecValueSquared<schedulerId>::ExtractedInt(
            extractColumn(purchaseValueSquaredShares, i))));
  }

  result.testReach = SecBit<schedulerId>(
      typename SecBit<schedulerId>::ExtractedBit(testReachShares));

  return result;
}

template <int schedulerId>
template <typename T>
std::string LiftGameProcessedData<schedulerId>::joinColumn(
    const std::vector<std::vector<T>>& data,
    size_t columnIndex) {
  if (data.size() == 0) {
    return "[]";
  } else if (data.size() == 1) {
    return "[" + std::to_string(data[0][columnIndex]) + "]";
  } else {
    std::string result = "[";
    for (size_t row = 0; row < data.size() - 1; row++) {
      result += std::to_string(data[row][columnIndex]) + ",";
    }

    result += std::to_string(data[data.size() - 1][columnIndex]) + "]";
    return result;
  }
}

template <int schedulerId>
template <typename T>
std::vector<T> LiftGameProcessedData<schedulerId>::extractColumn(
    const std::vector<std::vector<T>>& data,
    size_t columnIndex) {
  std::vector<T> result;
  result.reserve(data.size());
  for (size_t row = 0; row < data.size(); row++) {
    result.push_back(data[row][columnIndex]);
  }
  return result;
}

template <int schedulerId>
template <typename T>
std::vector<std::vector<T>> LiftGameProcessedData<schedulerId>::transpose(
    const std::vector<std::vector<T>>& data) {
  std::vector<std::vector<T>> result;
  result.reserve(data[0].size());
  for (size_t column = 0; column < data[0].size(); column++) {
    result.push_back(extractColumn(data, column));
  }
  return result;
}

template <int schedulerId>
std::vector<std::string> LiftGameProcessedData<schedulerId>::splitValueArray(
    const std::string& str) {
  auto innerString = str.substr(1, str.size() - 1);
  std::vector<std::string> values =
      private_measurement::csv::splitByComma(innerString, false);
  return values;
}

template <int schedulerId>
std::function<
    void(const std::vector<std::string>&, const std::vector<std::string>&)>
LiftGameProcessedData<schedulerId>::readParamsLine(
    LiftGameProcessedData<schedulerId>& result) {
  return [&result](
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
        XLOG(WARNING) << "Warning: Unknown column in csv: " << column;
      }
    }
  };
}

template <int schedulerId>
std::function<
    void(const std::vector<std::string>&, const std::vector<std::string>&)>
LiftGameProcessedData<schedulerId>::readSharesLine(
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
    std::vector<bool>& testReachShares) {
  return [&result,
          &indexShares,
          &testIndexShares,
          &opportunityTimestampsShares,
          &isValidOpportunityTimestampShares,
          &purchaseTimestampShares,
          &thresholdTimestampShares,
          &anyValidPurchaseTimestampShares,
          &purchaseValueShares,
          &purchaseValueSquaredShares,
          &testReachShares](
             const std::vector<std::string>& header,
             const std::vector<std::string>& parts) {
    result.numRows++;
    for (size_t i = 0; i < header.size(); i++) {
      auto column = header[i];
      auto value = parts[i];
      if (column == "indexShares") {
        indexShares.emplace_back();
        for (const auto& indexShare : splitValueArray(value)) {
          indexShares.back().push_back(std::stoul(indexShare));
        }
      } else if (column == "testIndexShares") {
        testIndexShares.emplace_back();
        for (const auto& testIndexShare : splitValueArray(value)) {
          testIndexShares.back().push_back(std::stoul(testIndexShare));
        }
      } else if (column == "opportunityTimestamps") {
        opportunityTimestampsShares.push_back(std::stoull(value));
      } else if (column == "isValidOpportunityTimestamp") {
        isValidOpportunityTimestampShares.push_back(std::stoul(value));
      } else if (column == "purchaseTimestamps") {
        purchaseTimestampShares.emplace_back();
        for (const auto& purchaseTimestampShare : splitValueArray(value)) {
          purchaseTimestampShares.back().push_back(
              std::stoull(purchaseTimestampShare));
        }
      } else if (column == "thresholdTimestamps") {
        thresholdTimestampShares.emplace_back();
        for (const auto& thresholdTimestampShare : splitValueArray(value)) {
          thresholdTimestampShares.back().push_back(
              std::stoull(thresholdTimestampShare));
        }
      } else if (column == "anyValidPurchaseTimestamp") {
        anyValidPurchaseTimestampShares.push_back(std::stoul(value));
      } else if (column == "purchaseValues") {
        purchaseValueShares.emplace_back();
        for (const auto& purchaseValueShare : splitValueArray(value)) {
          purchaseValueShares.back().push_back(std::stoll(purchaseValueShare));
        }
      } else if (column == "purchaseValueSquared") {
        purchaseValueSquaredShares.emplace_back();
        for (const auto& purchaseValueSquaredShare : splitValueArray(value)) {
          purchaseValueSquaredShares.back().push_back(
              std::stoll(purchaseValueSquaredShare));
        }
      } else if (column == "testReach") {
        testReachShares.push_back(std::stoul(value));
      } else if (column != "id_") {
        XLOG(WARNING) << "Warning: Unknown column in csv: " << column;
      }
    }
  };
}

} // namespace private_lift
