/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cmath>
#include <unordered_map>
#include <vector>

#include <emp-sh2pc/emp-sh2pc.h>

#include "fbpcs/emp_games/common/EmpOperationUtil.h"
#include "fbpcs/emp_games/common/PrivateData.h"
#include "fbpcs/emp_games/common/SecretSharing.h"
#include "fbpcs/emp_games/lift/calculator/LiftInputData.h"
#include "fbpcs/emp_games/lift/calculator/OutputMetricsData.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace private_lift {
/*
 * This class handles computation of Lift metrics from an input dataset.
 * We operate under the idiom of RAII: if you successfully construct an object
 * of this type, all of the metrics will have already been computed.
 */
template <int MY_ROLE>
class OutputMetrics {
 public:
  enum class GroupType { TEST, CONTROL };

  // Constructor. From an InputData object, calculate all output metrics
  OutputMetrics(
      const LiftInputData& inputData,
      bool isConversionLift,
      bool useXorEncryption,
      int32_t numConversionsPerUser)
      : inputData_{inputData},
        df_{inputData.getDf()},
        n_{inputData.size()},
        isConversionLift_{isConversionLift},
        useXorEncryption_{useXorEncryption},
        numConversionsPerUser_{numConversionsPerUser} {}

  std::string playGame();

  const OutputMetricsData& getMetrics() const {
    return metrics_;
  }

  const std::unordered_map<int64_t, OutputMetricsData>& getcohortMetrics()
      const {
    return cohortMetrics_;
  }

  const std::unordered_map<int64_t, OutputMetricsData>& getPublisherBreakdowns()
      const {
    return publisherBreakdowns_;
  }

  int64_t getNumPublisherBreakdowns() const {
    return numPublisherBreakdowns_;
  }

  int64_t getNumPartnerCohorts() const {
    return numPartnerCohorts_;
  }

  bool shouldUseXorEncryption() const {
    return useXorEncryption_;
  }

  void writeOutputToFile(std::ostream& outfile);

  std::string toJson() const;

 private:
  std::string getGroupTypeStr(GroupType groupType) {
    if (groupType == GroupType::TEST) {
      return "test";
    }
    return "control";
  }

  // Make sure input files have the same size
  void validateNumRows();

  // Initialize the number of groups that will be used for cohort computations
  // The publisher shares the number of groups for publisher breakdowns.
  // The partner shares the number of groups from its inputData and the *number
  // of groups* is revealed, but not the identities of those groups.
  void initNumGroups();

  // Initialize whether or not value-based calculations should be entirely
  // skipped. This happens if we're dealing with a valueless objective.
  void initShouldSkipValues();

  // Initialize the number of bits that will be used to compute purchase values.
  // The partner shares the maximum number of bits that will be needed for a
  // private integer sum (worst case of assuming all conversions valid and in
  // one side of the study).
  void initBitsForValues();

  // Calculate all metrics (helper function)
  void calculateAll();

  // Calculate valid purchases (oppTs < purchaseTs + 10)
  std::vector<std::vector<emp::Bit>> calculateValidPurchases();

  // Calculate component Lift statistics for the test/control group
  void calculateStatistics(
      const OutputMetrics::GroupType& groupType,
      const std::vector<std::vector<emp::Integer>>& purchaseValueArrays,
      const std::vector<std::vector<emp::Integer>>& purchaseValueSquaredArrays,
      const std::vector<std::vector<emp::Bit>>& validPurchaseArrays);

  // Test/Control population: oppFlag & testFlag/controlFlag
  std::vector<emp::Bit> calculatePopulation(
      const OutputMetrics::GroupType&,
      const std::vector<int64_t>);

  // Test/Control events: testPopulation/controlPopulation & validPurchase
  std::vector<std::vector<emp::Bit>> calculateEvents(
      const OutputMetrics::GroupType& groupType,
      const std::vector<emp::Bit>& populationBits,
      const std::vector<std::vector<emp::Bit>>& validPurchaseArrays);

  // Test/Control impressions: testImpressions/controlImpressions
  std::vector<emp::Bit> calculateImpressions(
      const OutputMetrics::GroupType& groupType,
      const std::vector<emp::Bit>& populationBits);

  // Reached conversions: (numImpressions > 0) & isReached
  void calculateReachedConversions(
      const OutputMetrics::GroupType& groupType,
      const std::vector<std::vector<emp::Bit>>& validPurchaseArrays,
      const std::vector<emp::Bit>& reachedArray);

  // Test/control match count: testPopulation/Control population &
  void calculateMatchCount(
      const OutputMetrics::GroupType& groupType,
      const std::vector<emp::Bit>& populationBits,
      const std::vector<std::vector<emp::Integer>>& purchaseValueArrays);

  // Test/Control value: testPurchaser/controlPurchaser ? purchaseValue : 0
  // Reached value: isReached ? purchaseValue : 0
  void calculateValue(
      const OutputMetrics::GroupType& groupType,
      const std::vector<std::vector<emp::Integer>>& purchaseValueArrays,
      const std::vector<std::vector<emp::Bit>>& testEventArrays,
      const std::vector<emp::Bit>& reachedArray);

  // Test/Control value squared:
  // sum(testPurchaser/controlPurchaser ? purchaseValue : 0)^2
  void calculateValueSquared(
      const OutputMetrics::GroupType& groupType,
      const std::vector<std::vector<emp::Integer>>& purchaseValueSquaredArrays,
      const std::vector<std::vector<emp::Bit>>& eventArrays);

  /**
   * Compute a private sum on a vector of bits, revealing to both parties at the
   * end.
   */
  int64_t sum(const std::vector<emp::Bit>& in) const;

  /**
   * Compute a private sum on a vector of integers, revealing to both parties at
   * the end.
   */
  int64_t sum(const std::vector<emp::Integer>& in) const;

  /**
   * Compute a private sum on a vector of vectors of bits, revealing to both
   * parties at the end.
   */
  int64_t sum(const std::vector<std::vector<emp::Bit>>& in) const;

  /**
   * Compute a private sum on a vector of vectors of integers, revealing to both
   * parties at the end.
   */
  int64_t sum(const std::vector<std::vector<emp::Integer>>& in) const;

  const LiftInputData& inputData_;
  const df::DataFrame &df_;
  int64_t n_;
  bool isConversionLift_;
  bool useXorEncryption_;
  bool shouldSkipValues_;
  int32_t numConversionsPerUser_;
  int64_t numPublisherBreakdowns_;
  int64_t numPartnerCohorts_;
  int64_t valueBits_;
  int64_t valueSquaredBits_;
  OutputMetricsData metrics_{isConversionLift_};

  std::unordered_map<int64_t, std::vector<emp::Bit>> publisherBitmasks_;
  std::unordered_map<int64_t, std::vector<emp::Bit>> partnerBitmasks_;
  std::unordered_map<int64_t, OutputMetricsData> cohortMetrics_;
  std::unordered_map<int64_t, OutputMetricsData> publisherBreakdowns_;

  template <class T>
  T reveal(const emp::Integer& empInteger) const;
};

} // namespace private_lift

#include "OutputMetrics.hpp"
