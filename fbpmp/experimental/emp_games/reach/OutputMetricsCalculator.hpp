/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <emp-sh2pc/emp-sh2pc.h>
#include <glog/logging.h>

#include <fbpmp/emp_games/common/EmpOperationUtil.h>
#include <fbpmp/emp_games/common/PrivateData.h>
#include <fbpmp/emp_games/common/SecretSharing.h>

namespace measurement::private_reach {

constexpr int INT_SIZE = 64;
constexpr int PUBLISHER = emp::ALICE;
constexpr int PARTNER = emp::BOB;

template <int MY_ROLE>
constexpr auto privatelyShareBitsFromPublisher =
    private_measurement::secret_sharing::privatelyShareBitsFromAlice<MY_ROLE>;

template <int MY_ROLE>
constexpr auto privatelyShareBitsFromPartner =
    private_measurement::secret_sharing::privatelyShareBitsFromBob<MY_ROLE>;

template <int MY_ROLE>
void OutputMetricsCalculator<MY_ROLE>::calculateAll() {
  LOG(INFO) << "Start calculation of output metrics";
  calculateReach();
  calculateFrequencyHistogram();
}

template <int MY_ROLE>
void OutputMetricsCalculator<MY_ROLE>::calculateReach() {
  auto reachBits = privatelyShareBitsFromPublisher<MY_ROLE>(
      inputData_.bitMaskForReached(), n_);
  for (auto i = 0; i < numCohorts_; ++i) {
    auto cohortMask = cohortBitmasks_.at(i);
    auto cohortBits = private_measurement::secret_sharing::multiplyBitmask(
        reachBits, cohortMask);
    if (useXOREncryption_) {
      cohortMetrics_[i].reach =
          private_measurement::emp_utils::sum<emp::XOR>(cohortBits);
    } else {
      cohortMetrics_[i].reach =
          private_measurement::emp_utils::sum<emp::PUBLIC>(cohortBits);
    }
  }
}

template <int MY_ROLE>
void OutputMetricsCalculator<MY_ROLE>::calculateFrequencyHistogram() {
  for (auto i = 0; i < numCohorts_; ++i) {
    LOG(INFO) << "Start frequency computation for cohort [" << i + 1 << " / "
              << numCohorts_ << "]";
    auto cohortMask = cohortBitmasks_.at(i);
    // TODO: A great extension here would be to use frequency *bins*
    // NOTE: We use <= maxFrequency_ since the max value *is* valid
    for (auto freq = 0; freq <= maxFrequency_; ++freq) {
      auto freqMask = frequencyBitmasks_.at(freq);
      auto freqBits = private_measurement::secret_sharing::multiplyBitmask(
          cohortMask, freqMask);
      if (useXOREncryption_) {
        cohortMetrics_.at(i).frequencyHistogram[freq] =
            private_measurement::emp_utils::sum<emp::XOR>(freqBits);
      } else {
        cohortMetrics_.at(i).frequencyHistogram[freq] =
            private_measurement::emp_utils::sum<emp::PUBLIC>(freqBits);
      }
    }
  }
}

template <int MY_ROLE>
void OutputMetricsCalculator<MY_ROLE>::initMaxFrequency() {
  LOG(INFO) << "Send max frequency for histograms and frequency bitmask shares";
  auto maxFrequency = static_cast<int64_t>(inputData_.getMaxFrequency());
  emp::Integer maxFrequencyInteger{INT_SIZE, maxFrequency, PUBLISHER};
  maxFrequency_ = maxFrequencyInteger.reveal<int64_t>();
  // NOTE: We use <= maxFrequency_ since the max value *is* valid
  for (auto i = 0; i <= maxFrequency_; ++i) {
    frequencyBitmasks_.push_back(privatelyShareBitsFromPublisher<MY_ROLE>(
        inputData_.bitMaskForFrequency(i), n_));
  }
  LOG(INFO) << "Max frequency for frequency histogram: " << maxFrequency_;
}

template <int MY_ROLE>
void OutputMetricsCalculator<MY_ROLE>::initNumCohorts() {
  LOG(INFO) << "Set up number of cohorts and cohortId share";
  auto numCohorts = static_cast<int64_t>(inputData_.getNumCohorts());
  emp::Integer numCohortsInteger{INT_SIZE, numCohorts, PARTNER};
  numCohorts_ = numCohortsInteger.reveal<int64_t>();
  // We pre-share the bitmasks for each cohort since they will be used
  // multiple times throughout the computation
  for (auto i = 0; i < numCohorts_; ++i) {
    cohortBitmasks_.push_back(privatelyShareBitsFromPartner<MY_ROLE>(
        inputData_.bitMaskForCohort(i), n_));
  }
  LOG(INFO) << "Will be computing metrics for " << numCohorts_ << " cohorts";
}

} // namespace measurement::private_reach
