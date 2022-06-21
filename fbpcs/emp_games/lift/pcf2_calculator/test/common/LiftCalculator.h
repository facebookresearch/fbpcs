/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <fstream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include <fbpcs/emp_games/common/Csv.h>
#include <fbpcs/emp_games/lift/common/GroupedLiftMetrics.h>
#include <sys/types.h>
#include "../../OutputMetricsData.h"

namespace private_lift {

inline constexpr uint64_t kDefaultEpochOffset = 1546300800;

class LiftCalculator {
 public:
  LiftCalculator(
      uint64_t numCohorts,
      uint64_t numPublisherBreakdown,
      uint64_t epoch)
      : numCohorts_(numCohorts),
        numPublisherBreakdown_(numPublisherBreakdown),
        epoch_(epoch) {}

  LiftCalculator()
      : numCohorts_(kNumDefaultCohorts),
        numPublisherBreakdown_(kNumPublisherBreakdown),
        epoch_(kDefaultEpochOffset) {}
  std::unordered_map<std::string, int32_t> mapColToIndex(
      const std::vector<std::string>& headerPublisher,
      const std::vector<std::string>& headerPartner) const;
  GroupedLiftMetrics compute(
      std::ifstream& inFilePublisher,
      std::ifstream& inFilePartner,
      std::unordered_map<std::string, int32_t>& colNameToIndex,
      int32_t tsOffset,
      bool useAdvancedLift = true) const;

 private:
  uint64_t numCohorts_, numPublisherBreakdown_, epoch_;

  // Parse input string with format [111,222,333,...]
  template <typename T>
  std::vector<T> parseArray(std::string array) const {
    auto innerString = array.substr(1, array.size() - 1);
    std::vector<T> out;
    auto values = private_measurement::csv::splitByComma(innerString, false);
    for (std::size_t i = 0; i < values.size(); ++i) {
      T parsed = 0;
      std::istringstream iss{values[i]};
      iss >> parsed;
      if (iss.fail()) {
        LOG(FATAL) << "Failed to parse '" << iss.str() << "' to "
                   << typeid(T).name();
      }
      out.push_back(parsed);
    }
    return out;
  }

  /**
   * Updates the `GroupedLiftMetrics` with metrics related to **Test** group.
   */
  void updateTestMetrics(
      GroupedLiftMetrics& glm,
      const uint64_t& opportunityTimestamp,
      const std::vector<uint64_t>& eventTimestamps,
      const uint8_t cohortId,
      const uint8_t breakdownId,
      const uint64_t tsOffset,
      const uint64_t numImpressions,
      const int64_t valuesIdx,
      const std::vector<int64_t>& values) const;

  /**
   * Updates the `GroupedLiftMetrics` with metrics related to **Control** group.
   */
  void updateControlMetrics(
      GroupedLiftMetrics& glm,
      const uint64_t& opportunityTimestamp,
      const std::vector<uint64_t>& eventTimestamps,
      const uint8_t cohortId,
      const uint8_t breakdownId,
      const uint64_t tsOffset,
      const int64_t valuesIdx,
      const std::vector<int64_t>& values) const;

  /**
   * Checks if the control event occurred after opportunity time and if was
   * attributed already increments match count and returns true.
   */
  bool checkAndUpdateControlMatchCount(
      GroupedLiftMetrics& glm,
      uint64_t opportunityTimestamp,
      uint64_t eventTimestamp,
      bool countedMatchAlready,
      uint8_t cohortId,
      uint8_t breakdownId) const;

  /**
   * Checks if the test event occurred after opportunity + tsOffset  time and if
   * was attributed already increments match count and returns true.
   */
  bool checkAndUpdateTestMatchCount(
      GroupedLiftMetrics& glm,
      uint64_t opportunityTimestamp,
      uint64_t eventTimestamp,
      bool countedMatchAlready,
      uint8_t cohortId,
      uint8_t breakdownId) const;

  /**
   * Checks if the control event occurred after opportunity + tsOffset time and
   * increments control conversions. If the conversion was a valid conversion,
   * then increments controlConverters.
   * @return true if opportunityTime < (event + tsOffset)
   */
  bool checkAndUpdateControlConversions(
      GroupedLiftMetrics& glm,
      uint64_t opportunityTimestamp,
      uint64_t eventTimestamp,
      int32_t tsOffset,
      bool converted,
      uint8_t cohortId,
      uint8_t breakdownId) const;

  /**
   * Checks if the test event occurred after opportunity + tsOffset time and
   * increments control conversions. If the conversion was a valid conversion,
   * then increments controlConverters.
   * @return true if opportunityTime < (event + tsOffset)
   */
  bool checkAndUpdateTestConversions(
      GroupedLiftMetrics& glm,
      uint64_t opportunityTimestamp,
      uint64_t eventTimestamp,
      int32_t tsOffset,
      bool converted,
      uint8_t cohortId,
      uint8_t breakdownId) const;

  std::tuple<uint64_t, bool> parseUint64OrDie(
      const std::string& column,
      const std::vector<std::string>& inLine,
      const std::unordered_map<std::string, int32_t>& colNameToIndex) const;

  std::vector<uint64_t> getAdjustedTimesEpochOffset(
      const std::vector<uint64_t>& timestamps) const;
};
} // namespace private_lift
