/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "InputData.h"

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include <fbpcs/emp_games/common/Csv.h>

namespace measurement::private_reach {

// All feature columns must be preprended with the kFeaturePrefix
static const std::string kFeaturePrefix = "feature_";

InputData::InputData(std::string filepath) {
  auto readLine = [&](const std::vector<std::string>& header,
                      const std::vector<std::string>& parts) {
    for (const auto& col : header) {
      if (col.rfind(kFeaturePrefix, 0) != std::string::npos) {
        featureHeader_.push_back(col);
      }
    }
    ++numRows_;
    addFromCSV(header, parts);
  };

  if (!private_measurement::csv::readCsv(filepath, readLine)) {
    LOG(FATAL) << "Failed to read input file " << filepath;
  }
}

std::vector<int64_t> InputData::bitMaskForCohort(int64_t cohortId) const {
  std::vector<int64_t> res(numRows_);
  if (cohortIds_.size() == res.size()) {
    LOG(INFO) << "Collecting bitmask for cohortId[" << cohortId << "]";
    for (auto i = 0; i < res.size(); ++i) {
      res[i] = cohortIds_.at(i) == cohortId ? 1 : 0;
    }
  }
  return res;
}

std::vector<int64_t> InputData::bitMaskForFrequency(int64_t frequency) const {
  std::vector<int64_t> res(numRows_);
  if (frequencies_.size() == res.size()) {
    LOG(INFO) << "Collecting bitmask for frequency[" << frequency << "]";
    for (auto i = 0; i < res.size(); ++i) {
      res[i] = frequencies_.at(i) == frequency ? 1 : 0;
    }
  }
  return res;
}

std::vector<int64_t> InputData::bitMaskForReached() const {
  std::vector<int64_t> res(numRows_);
  if (frequencies_.size() == res.size()) {
    LOG(INFO) << "Collecting bitmask for isReached";
    for (auto i = 0; i < res.size(); ++i) {
      res[i] = frequencies_.at(i) > 0 ? 1 : 0;
    }
  }
  return res;
}

void InputData::addFromCSV(
    const std::vector<std::string>& header,
    const std::vector<std::string>& parts) {
  std::vector<std::string> featureValues;

  for (auto i = 0; i < header.size(); ++i) {
    auto& column = header[i];
    auto& value = parts[i];

    std::istringstream iss{value};
    if (column == "frequency") {
      int64_t frequency;
      iss >> frequency;
      if (iss.fail()) {
        LOG(FATAL) << "Failed to parse " << value
                   << " as int64_t for frequency column";
      }
      maxFrequency_ = std::max(maxFrequency_, frequency);
      frequencies_.push_back(frequency);
    } else if (column.rfind(kFeaturePrefix, 0) != std::string::npos) {
      featureValues.push_back(value);
    }
  }

  // Finally, check which feature cohortId this rows belongs to. If we haven't
  // seen this cohort before, denote that it corresponds to a new cohortId
  if (featureHeader_.size() > 0 &&
      featuresToCohortId_.find(featureValues) == featuresToCohortId_.end()) {
    featuresToCohortId_[featureValues] = numCohorts_;
    cohortIdToFeatures_[numCohorts_] = featureValues;
    ++numCohorts_;
  }

  if (featureHeader_.size() > 0) {
    cohortIds_.push_back(featuresToCohortId_[featureValues]);
  }
}

} // namespace measurement::private_reach
