/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/common/Util.h"
namespace private_lift {

template <int schedulerId>
void Aggregator<schedulerId>::initOram() {
  // Initialize ORAM
  bool isPublisher = (myRole_ == common::PUBLISHER);
  numCohortGroups_ = std::max(2 * numPartnerCohorts_, uint32_t(2));

  if (numCohortGroups_ > 4) {
    // If the ORAM size is larger than 4, linear ORAM is less efficient
    // theoretically
    cohortUnsignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<false, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
  } else {
    cohortUnsignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<false, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumEvents() {
  XLOG(INFO) << "Aggregate events";
  // Aggregate across test/control and cohorts
  std::vector<std::vector<std::vector<bool>>> valueSharesArray;
  for (auto events : attributor_->getEvents()) {
    std::vector<std::vector<bool>> valueShares(
        valueWidth, std::vector<bool>(numRows_, 0));
    valueShares[0] = events.extractBit().getValue();
    valueSharesArray.push_back(std::move(valueShares));
  }
  auto oram = cohortUnsignedWriteOnlyOramFactory_->create(numCohortGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, true>(
      cohortIndexShares_, valueSharesArray, numCohortGroups_, std::move(oram));

  // Extract metrics
  auto cohortOutput = revealCohortOutput(aggregationOutput);
  metrics_.testEvents = std::get<0>(cohortOutput).at(0);
  metrics_.controlEvents = std::get<0>(cohortOutput).at(1);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testEvents = std::get<1>(cohortOutput).at(i);
    cohortMetrics_[i].controlEvents = std::get<2>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumConverters() {
  XLOG(INFO) << "Aggregate converters";
  // Aggregate across test/control and cohorts
  std::vector<std::vector<bool>> valueShares(
      valueWidth, std::vector<bool>(numRows_, 0));
  valueShares[0] = attributor_->getConverters().extractBit().getValue();
  auto oram = cohortUnsignedWriteOnlyOramFactory_->create(numCohortGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, false>(
      cohortIndexShares_, valueShares, numCohortGroups_, std::move(oram));

  // Extract metrics
  auto cohortOutput = revealCohortOutput(aggregationOutput);
  metrics_.testConverters = std::get<0>(cohortOutput).at(0);
  metrics_.controlConverters = std::get<0>(cohortOutput).at(1);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testConverters = std::get<1>(cohortOutput).at(i);
    cohortMetrics_[i].controlConverters = std::get<2>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumMatch() {
  XLOG(INFO) << "Aggregate matchCount";
  // Aggregate across test/control and cohorts
  std::vector<std::vector<bool>> valueShares(
      valueWidth, std::vector<bool>(numRows_, 0));
  valueShares[0] = attributor_->getMatch().extractBit().getValue();
  auto oram = cohortUnsignedWriteOnlyOramFactory_->create(numCohortGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, false>(
      cohortIndexShares_, valueShares, numCohortGroups_, std::move(oram));

  // Extract metrics
  auto cohortOutput = revealCohortOutput(aggregationOutput);
  metrics_.testMatchCount = std::get<0>(cohortOutput).at(0);
  metrics_.controlMatchCount = std::get<0>(cohortOutput).at(1);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testMatchCount = std::get<1>(cohortOutput).at(i);
    cohortMetrics_[i].controlMatchCount = std::get<2>(cohortOutput).at(i);
  }
}

template <int schedulerId>
template <bool isSigned, int8_t width, bool useVector>
std::vector<SecInt<schedulerId, isSigned, width>>
Aggregator<schedulerId>::aggregate(
    const std::vector<std::vector<bool>>& indexShares,
    ConditionalVector<std::vector<std::vector<bool>>, useVector>& valueShares,
    size_t oramSize,
    std::unique_ptr<
        fbpcf::mpc_std_lib::oram::IWriteOnlyOram<Intp<isSigned, width>>> oram)
    const {
  // aggregate using ORAM
  if constexpr (useVector) {
    for (size_t i = 0; i < valueShares.size(); ++i) {
      oram->obliviousAddBatch(indexShares, valueShares.at(i));
    }
  } else {
    oram->obliviousAddBatch(indexShares, valueShares);
  }
  std::vector<SecInt<schedulerId, isSigned, width>> output;
  for (size_t i = 0; i < oramSize; ++i) {
    NativeIntp<isSigned, width> additiveSum(oram->secretRead(i));
    // Convert additive shares to secret shares by inputting them into MPC
    // and adding them, then extracting the secret shares.
    auto publisherSum =
        SecInt<schedulerId, isSigned, width>(additiveSum, common::PUBLISHER);
    auto partnerSum =
        SecInt<schedulerId, isSigned, width>(additiveSum, common::PARTNER);
    output.push_back(publisherSum + partnerSum);
  }
  return output;
}

template <int schedulerId>
template <bool isSigned, int8_t width>
std::tuple<
    std::vector<NativeIntp<isSigned, width>>,
    std::vector<NativeIntp<isSigned, width>>,
    std::vector<NativeIntp<isSigned, width>>>
Aggregator<schedulerId>::revealCohortOutput(
    std::vector<SecInt<schedulerId, isSigned, width>> aggregationOutput) const {
  std::vector<NativeIntp<isSigned, width>> testCohortOutput;
  std::vector<NativeIntp<isSigned, width>> controlCohortOutput;
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    // Extract cohort metrics
    testCohortOutput.push_back(
        aggregationOutput.at(i).extractIntShare().getValue());
    controlCohortOutput.push_back(aggregationOutput.at(i + numPartnerCohorts_)
                                      .extractIntShare()
                                      .getValue());
  }

  // Initialize test/control metrics for the case where there are no partner
  // cohorts
  auto test = aggregationOutput.at(0);
  auto control =
      aggregationOutput.at(std::max(uint32_t(1), numPartnerCohorts_));
  for (size_t i = 1; i < numPartnerCohorts_; ++i) {
    // Compute test/control metrics by summing up cohort metrics for each
    // population
    test = test + aggregationOutput.at(i);
    control = control + aggregationOutput.at(i + numPartnerCohorts_);
  }
  std::vector<NativeIntp<isSigned, width>> testControlOutput;
  testControlOutput.push_back(test.extractIntShare().getValue());
  testControlOutput.push_back(control.extractIntShare().getValue());
  return std::make_tuple(
      testControlOutput, testCohortOutput, controlCohortOutput);
}

} // namespace private_lift
