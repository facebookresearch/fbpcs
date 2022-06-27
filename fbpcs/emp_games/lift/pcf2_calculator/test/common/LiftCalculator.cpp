/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <vector>

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
      LOG(ERROR) << "Failed to parse '" << iss.str() << "' to uint64_t";
      throw std::runtime_error("Parse error");
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
  uint8_t breakdownId = 0;
  uint8_t cohortId = 0;
  std::vector<uint64_t> eventTimestamps;
  std::vector<int64_t> values;

  std::string linePublisher;
  std::string linePartner;

  GroupedLiftMetrics groupedLiftMetrics(numCohorts_, numPublisherBreakdown_);

  groupedLiftMetrics.reset();

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
      LOG(ERROR) << "Empty publisher line";
      throw std::runtime_error("Empty publisher lines");
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

    std::tie(parsedVal, parseStatus) =
        parseUint64OrDie("breakdown_id", partsPublisher, colNameToIndex);
    breakdownId = parseStatus ? parsedVal : 0;
    if (numPublisherBreakdown_ > 0 && parseStatus)
      CHECK_LE(breakdownId, numPublisherBreakdown_)
          << " breakdownId has to be less than numPublisherBreakdown, check constructor of LiftCalculator.";

    if (partsPartner.empty()) {
      LOG(ERROR) << "Empty partner line";
      throw std::runtime_error("Empty partner line");
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
        LOG(ERROR) << "Size of event_timestamps (" << eventTimestamps.size()
                   << ") and values (" << values.size() << ") are inconsistent";
        throw std::runtime_error("Inconsistent size error");
      }
    }
    std::tie(parsedVal, parseStatus) =
        parseUint64OrDie("cohort_id", partsPartner, colNameToIndex);
    cohortId = parseStatus ? parsedVal : 0;
    if (numCohorts_ > 0 && parseStatus) {
      CHECK_LE(cohortId, numCohorts_)
          << " cohortId has to be less than numCohorts, check constructor of LiftCalculator.";
    }

    if (opportunity && opportunityTimestamp > 0) {
      uint64_t value_subsum = 0;
      uint64_t convCount = 0;
      bool converted = false;
      bool countedMatchAlready = false;
      if (testFlag) {
        updateTestMetrics(
            groupedLiftMetrics,
            opportunityTimestamp,
            eventTimestamps,
            cohortId,
            breakdownId,
            tsOffset,
            numImpressions,
            valuesIdx,
            values);
      } else {
        updateControlMetrics(
            groupedLiftMetrics,
            opportunityTimestamp,
            eventTimestamps,
            cohortId,
            breakdownId,
            tsOffset,
            valuesIdx,
            values);
      }
    }
  }

  return groupedLiftMetrics;
}

bool LiftCalculator::checkAndUpdateControlMatchCount(
    GroupedLiftMetrics& groupedLiftMetrics,
    uint64_t opportunityTimestamp,
    uint64_t eventTimestamp,
    bool countedMatchAlready,
    uint8_t cohortId,
    uint8_t breakdownId) const {
  if (opportunityTimestamp > 0 && eventTimestamp > 0 && !countedMatchAlready) {
    ++groupedLiftMetrics.metrics.controlMatchCount;
    if (numCohorts_ > 0)
      ++groupedLiftMetrics.cohortMetrics.at(cohortId).controlMatchCount;
    if (numPublisherBreakdown_ > 0)
      ++groupedLiftMetrics.publisherBreakdowns.at(breakdownId)
            .controlMatchCount;

    return true;
  }
  return false;
}

bool LiftCalculator::checkAndUpdateTestMatchCount(
    GroupedLiftMetrics& groupedLiftMetrics,
    uint64_t opportunityTimestamp,
    uint64_t eventTimestamp,
    bool countedMatchAlready,
    uint8_t cohortId,
    uint8_t breakdownId) const {
  if (opportunityTimestamp > 0 && eventTimestamp > 0 && !countedMatchAlready) {
    ++groupedLiftMetrics.metrics.testMatchCount;
    if (numCohorts_ > 0)
      ++groupedLiftMetrics.cohortMetrics.at(cohortId).testMatchCount;
    if (numPublisherBreakdown_ > 0)
      ++groupedLiftMetrics.publisherBreakdowns.at(breakdownId).testMatchCount;

    return true;
  }
  return false;
}

bool LiftCalculator::checkAndUpdateControlConversions(
    GroupedLiftMetrics& groupedLiftMetrics,
    uint64_t opportunityTimestamp,
    uint64_t eventTimestamp,
    int32_t tsOffset,
    bool converted,
    uint8_t cohortId,
    uint8_t breakdownId) const {
  if (opportunityTimestamp < eventTimestamp + tsOffset) {
    // Only record the first time the user has a valid conversion
    if (!converted) {
      ++groupedLiftMetrics.metrics.controlConverters;
      if (numCohorts_ > 0)
        ++groupedLiftMetrics.cohortMetrics.at(cohortId).controlConverters;
      if (numPublisherBreakdown_ > 0)
        ++groupedLiftMetrics.publisherBreakdowns.at(breakdownId)
              .controlConverters;
    }
    ++groupedLiftMetrics.metrics.controlConversions;

    if (numCohorts_ > 0)
      ++groupedLiftMetrics.cohortMetrics.at(cohortId).controlConversions;
    if (numPublisherBreakdown_ > 0)
      ++groupedLiftMetrics.publisherBreakdowns.at(breakdownId)
            .controlConversions;

    return true;
  }
  return false;
}

bool LiftCalculator::checkAndUpdateTestConversions(
    GroupedLiftMetrics& groupedLiftMetrics,
    uint64_t opportunityTimestamp,
    uint64_t eventTimestamp,
    int32_t tsOffset,
    bool converted,
    uint8_t cohortId,
    uint8_t breakdownId) const {
  if (opportunityTimestamp < eventTimestamp + tsOffset) {
    // Only record the first time the user has a valid conversion
    if (!converted) {
      ++groupedLiftMetrics.metrics.testConverters;
      if (numCohorts_ > 0)
        ++groupedLiftMetrics.cohortMetrics.at(cohortId).testConverters;
      if (numPublisherBreakdown_ > 0)
        ++groupedLiftMetrics.publisherBreakdowns.at(breakdownId).testConverters;
    }

    ++groupedLiftMetrics.metrics.testConversions;
    if (numCohorts_ > 0)
      ++groupedLiftMetrics.cohortMetrics.at(cohortId).testConversions;
    if (numPublisherBreakdown_ > 0)
      ++groupedLiftMetrics.publisherBreakdowns.at(breakdownId).testConversions;

    return true;
  }
  return false;
}

void LiftCalculator::updateControlMetrics(
    GroupedLiftMetrics& groupedLiftMetrics,
    const uint64_t& opportunityTimestamp,
    const std::vector<uint64_t>& eventTimestamps,
    const uint8_t cohortId,
    const uint8_t breakdownId,
    const uint64_t tsOffset,
    const int64_t valuesIdx,
    const std::vector<int64_t>& values) const {
  uint64_t value_subsum = 0;
  uint64_t convCount = 0;
  bool converted = false;
  bool countedMatchAlready = false;

  for (std::size_t i = 0; i < eventTimestamps.size(); ++i) {
    countedMatchAlready |= checkAndUpdateControlMatchCount(
        groupedLiftMetrics,
        opportunityTimestamp,
        eventTimestamps.at(i),
        countedMatchAlready,
        cohortId,
        breakdownId);

    if (checkAndUpdateControlConversions(
            groupedLiftMetrics,
            opportunityTimestamp,
            eventTimestamps.at(i),
            tsOffset,
            converted,
            cohortId,
            breakdownId)) {
      converted = true;
      convCount++;
      if (valuesIdx != -1) {
        // Only add values if the values column exists
        // (support valueless objectives)
        value_subsum += values.at(i);
      }
    }
  }
  uint64_t controlValueSquared = value_subsum * value_subsum;
  uint64_t controlNumConvSquared = convCount * convCount;
  groupedLiftMetrics.metrics.controlValue += value_subsum;
  groupedLiftMetrics.metrics.controlValueSquared += controlValueSquared;
  groupedLiftMetrics.metrics.controlNumConvSquared += controlNumConvSquared;
  if (numCohorts_ > 0) {
    groupedLiftMetrics.cohortMetrics.at(cohortId).controlValue += value_subsum;
    groupedLiftMetrics.cohortMetrics.at(cohortId).controlValueSquared +=
        controlValueSquared;
    groupedLiftMetrics.cohortMetrics.at(cohortId).controlNumConvSquared +=
        controlNumConvSquared;
  }
  if (numPublisherBreakdown_ > 0) {
    groupedLiftMetrics.publisherBreakdowns.at(breakdownId).controlValue +=
        value_subsum;
    groupedLiftMetrics.publisherBreakdowns.at(breakdownId)
        .controlValueSquared += controlValueSquared;
    groupedLiftMetrics.publisherBreakdowns.at(breakdownId)
        .controlNumConvSquared += controlNumConvSquared;
  }
}

void LiftCalculator::updateTestMetrics(
    GroupedLiftMetrics& groupedLiftMetrics,
    const uint64_t& opportunityTimestamp,
    const std::vector<uint64_t>& eventTimestamps,
    const uint8_t cohortId,
    const uint8_t breakdownId,
    const uint64_t tsOffset,
    const uint64_t numImpressions,
    const int64_t valuesIdx,
    const std::vector<int64_t>& values) const {
  uint64_t value_subsum = 0;
  uint64_t convCount = 0;
  bool converted = false;
  bool countedMatchAlready = false;

  for (std::size_t i = 0; i < eventTimestamps.size(); ++i) {
    countedMatchAlready |= checkAndUpdateTestMatchCount(
        groupedLiftMetrics,
        opportunityTimestamp,
        eventTimestamps.at(i),
        countedMatchAlready,
        cohortId,
        breakdownId);

    if (checkAndUpdateTestConversions(
            groupedLiftMetrics,
            opportunityTimestamp,
            eventTimestamps.at(i),
            tsOffset,
            converted,
            cohortId,
            breakdownId)) {
      converted = true;
      convCount++;
      if (numImpressions > 0) {
        ++groupedLiftMetrics.metrics.reachedConversions;
        if (numCohorts_ > 0)
          ++groupedLiftMetrics.cohortMetrics.at(cohortId).reachedConversions;
        if (numPublisherBreakdown_ > 0)
          ++groupedLiftMetrics.publisherBreakdowns.at(breakdownId)
                .reachedConversions;
      }
      if (valuesIdx != -1) {
        // Only add values if the values column exists
        // (support valueless objectives)
        value_subsum += values.at(i);
      }
    }
  }

  if (numImpressions > 0) {
    groupedLiftMetrics.metrics.reachedValue += value_subsum;

    if (numCohorts_ > 0)
      groupedLiftMetrics.cohortMetrics.at(cohortId).reachedValue +=
          value_subsum;
    if (numPublisherBreakdown_ > 0)
      groupedLiftMetrics.publisherBreakdowns.at(breakdownId).reachedValue +=
          value_subsum;
  }

  uint64_t testValueSquared = value_subsum * value_subsum;
  uint64_t testNumConvSquared = convCount * convCount;

  groupedLiftMetrics.metrics.testValue += value_subsum;
  groupedLiftMetrics.metrics.testValueSquared += testValueSquared;
  groupedLiftMetrics.metrics.testNumConvSquared += testNumConvSquared;

  if (numCohorts_ > 0) {
    groupedLiftMetrics.cohortMetrics.at(cohortId).testValue += value_subsum;
    groupedLiftMetrics.cohortMetrics.at(cohortId).testValueSquared +=
        testValueSquared;
    groupedLiftMetrics.cohortMetrics.at(cohortId).testNumConvSquared +=
        testNumConvSquared;
  }

  if (numPublisherBreakdown_ > 0) {
    groupedLiftMetrics.publisherBreakdowns.at(breakdownId).testValue +=
        value_subsum;
    groupedLiftMetrics.publisherBreakdowns.at(breakdownId).testValueSquared +=
        testValueSquared;
    groupedLiftMetrics.publisherBreakdowns.at(breakdownId).testNumConvSquared +=
        testNumConvSquared;
  }
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
