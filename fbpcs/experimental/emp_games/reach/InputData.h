/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace measurement::private_reach {

class InputData {
 public:
  explicit InputData(std::string filepath);
  std::vector<int64_t> bitMaskForCohort(int64_t cohortId) const;
  std::vector<int64_t> bitMaskForFrequency(int64_t frequency) const;
  std::vector<int64_t> bitMaskForReached() const;

  const std::vector<int64_t> getFrequencies() const {
    return frequencies_;
  }

  const std::vector<int64_t>& getCohortIds() const {
    return cohortIds_;
  }

  const std::unordered_map<int64_t, std::vector<std::string>>&
  getCohortIdToFeatures() const {
    return cohortIdToFeatures_;
  }

  int64_t getMaxFrequency() const {
    return maxFrequency_;
  }

  int64_t getNumCohorts() const {
    return numCohorts_;
  }

  int64_t getNumRows() const {
    return numRows_;
  }

  const std::vector<std::string>& getFeatureHeader() const {
    return featureHeader_;
  }

 private:
  void addFromCSV(
      const std::vector<std::string>& header,
      const std::vector<std::string>& parts);

  std::vector<int64_t> frequencies_;
  std::vector<int64_t> cohortIds_;

  std::vector<std::string> featureHeader_;
  std::unordered_map<int64_t, std::vector<std::string>> cohortIdToFeatures_;
  std::map<std::vector<std::string>, int64_t> featuresToCohortId_;
  int64_t maxFrequency_;
  int64_t numCohorts_;

  int64_t numRows_;
};

} // namespace measurement::private_reach
