/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/OutputMetricsData.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/test/common/LiftCalculator.h"

namespace private_lift {
std::unordered_map<std::string, int32_t> LiftCalculator::mapColToIndex(
    const std::vector<std::string>& headerPublisher,
    const std::vector<std::string>& headerPartner) const {
  std::unordered_map<std::string, int32_t> colNameToIndex;
  int32_t index;
  for (auto it = headerPublisher.begin(); it != headerPublisher.end(); ++it) {
    index = std::distance(headerPublisher.begin(), it);
    colNameToIndex[*it] = index;
  }
  for (auto it = headerPartner.begin(); it != headerPartner.end(); ++it) {
    index = std::distance(headerPartner.begin(), it);
    colNameToIndex[*it] = index;
  }
  return colNameToIndex;
}

std::tuple<uint64_t, bool> LiftCalculator::parseUint64OrDie(
    const std::string& column,
    const std::vector<std::string>& inParts,
    const std::unordered_map<std::string, int32_t>& colNameToIndex) const {
  uint64_t res;
  if (colNameToIndex.find(column) != colNameToIndex.end()) {
    std::istringstream iss(inParts.at(colNameToIndex.at(column)));
    iss >> res;
    if (iss.fail()) {
      LOG(FATAL) << "Failed to parse '" << iss.str() << "' to uint64_t";
    } else {
      return {res, true};
    }
  }
  return {res, false};
}

GroupedLiftMetrics LiftCalculator::compute(
    std::ifstream& inFilePublisher,
    std::ifstream& inFilePartner,
    std::unordered_map<std::string, int32_t>& colNameToIndex,
    int32_t tsOffset,
    bool useAdvancedLift) const {
  uint64_t opportunity = 0;
  uint64_t numImpressions = 0;
  uint64_t numClicks = 0;
  uint64_t totalSpend = 0;
  uint64_t testFlag = 0;
  uint64_t opportunityTimestamp = 0;
  std::vector<uint64_t> eventTimestamps;
  std::vector<int64_t> values;

  std::string linePublisher;
  std::string linePartner;

  GroupedLiftMetrics glm(numCohorts_, numPublisherBreakdown_);

  glm.reset();

  uint64_t parsedVal;
  bool parseStatus;

  // Read line by line, at the same time compute metrics
  while (getline(inFilePublisher, linePublisher) &&
         getline(inFilePartner, linePartner)) {
    auto partsPublisher =
        private_measurement::csv::splitByComma(linePublisher, true);
    auto partsPartner =
        private_measurement::csv::splitByComma(linePartner, true);

    if (partsPublisher.empty()) {
      LOG(FATAL) << "Empty publisher line";
    }

    // Opportunity is actually an optional column
    std::tie(parsedVal, parseStatus) =
        parseUint64OrDie("opportunity", partsPublisher, colNameToIndex);
    opportunity = parseStatus ? parsedVal : 1;

    std::tie(parsedVal, parseStatus) =
        parseUint64OrDie("test_flag", partsPublisher, colNameToIndex);
    testFlag = parseStatus ? parsedVal : 0;

    std::tie(parsedVal, parseStatus) = parseUint64OrDie(
        "opportunity_timestamp", partsPublisher, colNameToIndex);
    opportunityTimestamp =
        parseStatus ? (parsedVal > epoch_ ? (parsedVal - epoch_) : 0) : 0;

    std::tie(parsedVal, parseStatus) =
        parseUint64OrDie("num_clicks", partsPublisher, colNameToIndex);
    numClicks = parseStatus ? parsedVal : 0;

    std::tie(parsedVal, parseStatus) =
        parseUint64OrDie("num_impressions", partsPublisher, colNameToIndex);
    numImpressions = parseStatus ? parsedVal : 0;

    std::tie(parsedVal, parseStatus) =
        parseUint64OrDie("total_spend", partsPublisher, colNameToIndex);
    totalSpend = parseStatus ? parsedVal : 0;

    if (partsPartner.empty()) {
      LOG(FATAL) << "Empty partner line";
    }
    eventTimestamps = parseArray<uint64_t>(
        partsPartner.at(colNameToIndex.at("event_timestamps")));
    eventTimestamps = getAdjustedTimesEpochOffset(eventTimestamps);
    auto valuesIdx = colNameToIndex.find("values") != colNameToIndex.end()
        ? colNameToIndex.at("values")
        : -1;
    if (valuesIdx != -1) {
      values = parseArray<int64_t>(partsPartner.at(valuesIdx));

      if (eventTimestamps.size() != values.size()) {
        LOG(FATAL) << "Size of event_timestamps (" << eventTimestamps.size()
                   << ") and values (" << values.size() << ") are inconsistent";
      }
    }

    if (opportunity && opportunityTimestamp > 0) {
      uint64_t value_subsum = 0;
      uint64_t convCount = 0;
      bool converted = false;
      bool countedMatchAlready = false;
      if (testFlag) {
        for (std::size_t i = 0; i < eventTimestamps.size(); ++i) {
          if (opportunityTimestamp > 0 && eventTimestamps.at(i) > 0 &&
              !countedMatchAlready) {
            ++glm.metrics.testMatchCount;
            countedMatchAlready = true;
          }
          if (opportunityTimestamp < eventTimestamps.at(i) + tsOffset) {
            // Only record the first time the user has a valid conversion
            if (!converted) {
              ++glm.metrics.testConverters;
            }
            ++glm.metrics.testConversions;
            ++convCount;
            converted = true;
            if (numImpressions > 0) {
              ++glm.metrics.reachedConversions;
            }
            if (valuesIdx != -1) {
              // Only add values if the values column exists
              // (support valueless objectives)
              value_subsum += values.at(i);
            }
          }
        }
        glm.metrics.testValue += value_subsum;
        if (numImpressions > 0) {
          glm.metrics.reachedValue += value_subsum;
        }
        glm.metrics.testValueSquared += value_subsum * value_subsum;
        glm.metrics.testNumConvSquared += convCount * convCount;
      } else {
        for (std::size_t i = 0; i < eventTimestamps.size(); ++i) {
          if (opportunityTimestamp > 0 && eventTimestamps.at(i) > 0 &&
              !countedMatchAlready) {
            ++glm.metrics.controlMatchCount;
            countedMatchAlready = true;
          }
          if (opportunityTimestamp < eventTimestamps.at(i) + tsOffset) {
            // Only record the first time the user has a valid conversion
            if (!converted) {
              ++glm.metrics.controlConverters;
            }
            ++glm.metrics.controlConversions;
            ++convCount;
            converted = true;
            if (valuesIdx != -1) {
              // Only add values if the values column exists
              // (support valueless objectives)
              value_subsum += values.at(i);
            }
          }
        }
        glm.metrics.controlValue += value_subsum;
        glm.metrics.controlValueSquared += value_subsum * value_subsum;
        glm.metrics.controlNumConvSquared += convCount * convCount;
      }
    }
  }

  return glm;
}

std::vector<uint64_t> LiftCalculator::getAdjustedTimesEpochOffset(
    const std::vector<uint64_t>& timestamps) const {
  std::vector<uint64_t> res(timestamps.size(), 0);
  for (size_t i = 0; i < timestamps.size(); ++i) {
    if (timestamps[i] > epoch_) {
      res[i] = timestamps[i] - epoch_;
    }
  }
  return res;
}

} // namespace private_lift
