/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "InputData.h"
#include "OutputMetrics.h"

#include <unordered_map>
#include <vector>

namespace measurement::private_reach {

template <int MY_ROLE>
class OutputMetricsCalculator {
 public:
  OutputMetricsCalculator(const InputData& inputData, bool useXOREncryption)
      : inputData_{inputData},
        n_{static_cast<int64_t>(inputData.getNumRows())},
        useXOREncryption_{useXOREncryption} {
    initMaxFrequency();
    initNumCohorts();
  }

  void calculateAll();

  const std::unordered_map<int64_t, OutputMetrics>& getCohortMetrics() const {
    return cohortMetrics_;
  }

 private:
  void initMaxFrequency();
  void initNumCohorts();

  void calculateReach();
  void calculateFrequencyHistogram();

  const InputData& inputData_;
  int64_t n_;
  bool useXOREncryption_;

  int64_t numCohorts_;
  int64_t maxFrequency_;
  std::vector<std::vector<emp::Bit>> cohortBitmasks_;
  std::vector<std::vector<emp::Bit>> frequencyBitmasks_;

  std::unordered_map<int64_t, OutputMetrics> cohortMetrics_;
};

} // namespace measurement::private_reach

#include "OutputMetricsCalculator.hpp"
