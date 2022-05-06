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
  numTestCohortGroups_ = std::max(numPartnerCohorts_ + 1, uint32_t(2));

  if (numCohortGroups_ > 4) {
    // If the ORAM size is larger than 4, linear ORAM is less efficient
    // theoretically
    cohortUnsignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<false, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
    cohortSignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<true, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
    valueSquaredWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<false, valueSquaredWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
  } else {
    cohortUnsignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<false, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
    cohortSignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<true, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
    valueSquaredWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<false, valueSquaredWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
  }

  if (numTestCohortGroups_ > 4) {
    testCohortUnsignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<false, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
    testCohortSignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<true, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
  } else {
    testCohortUnsignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<false, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
    testCohortSignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<true, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
  }
}

template <int schedulerId>
std::string Aggregator<schedulerId>::toJson() const {
  GroupedLiftMetrics groupedLiftMetrics;
  groupedLiftMetrics.metrics = metrics_.toLiftMetrics();
  std::transform(
      cohortMetrics_.begin(),
      cohortMetrics_.end(),
      std::back_inserter(groupedLiftMetrics.cohortMetrics),
      [](auto const& p) { return p.second.toLiftMetrics(); });
  return groupedLiftMetrics.toJson();
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
void Aggregator<schedulerId>::sumNumConvSquared() {
  XLOG(INFO) << "Aggregate numConvSquared";
  // Aggregate across test/control and cohorts
  auto valueShares =
      attributor_->getNumConvSquared().extractIntShare().getBooleanShares();
  auto oram = cohortUnsignedWriteOnlyOramFactory_->create(numCohortGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, false>(
      cohortIndexShares_, valueShares, numCohortGroups_, std::move(oram));

  // Extract metrics
  auto cohortOutput = revealCohortOutput(aggregationOutput);
  metrics_.testNumConvSquared = std::get<0>(cohortOutput).at(0);
  metrics_.controlNumConvSquared = std::get<0>(cohortOutput).at(1);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testNumConvSquared = std::get<1>(cohortOutput).at(i);
    cohortMetrics_[i].controlNumConvSquared = std::get<2>(cohortOutput).at(i);
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
void Aggregator<schedulerId>::sumReachedConversions() {
  XLOG(INFO) << "Aggregate reachedConversions";
  // Aggregate across test/control and cohorts
  std::vector<std::vector<std::vector<bool>>> valueSharesArray;
  for (auto events : attributor_->getReachedConversions()) {
    std::vector<std::vector<bool>> valueShares(
        valueWidth, std::vector<bool>(numRows_, 0));
    valueShares[0] = events.extractBit().getValue();
    valueSharesArray.push_back(std::move(valueShares));
  }
  auto oram =
      testCohortUnsignedWriteOnlyOramFactory_->create(numTestCohortGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, true>(
      testCohortIndexShares_,
      valueSharesArray,
      numTestCohortGroups_,
      std::move(oram));

  // Extract metrics
  auto cohortOutput = revealTestCohortOutput(aggregationOutput);
  metrics_.reachedConversions = std::get<0>(cohortOutput);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].reachedConversions = std::get<1>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumValues() {
  XLOG(INFO) << "Aggregate values";
  // Aggregate across test/control and cohorts
  std::vector<std::vector<std::vector<bool>>> valueSharesArray;
  for (auto input : attributor_->getValues()) {
    auto valueShares = input.extractIntShare().getBooleanShares();
    valueSharesArray.push_back(std::move(valueShares));
  }
  auto oram = cohortSignedWriteOnlyOramFactory_->create(numCohortGroups_);
  auto aggregationOutput = aggregate<true, valueWidth, true>(
      cohortIndexShares_, valueSharesArray, numCohortGroups_, std::move(oram));

  // Extract metrics
  auto cohortOutput = revealCohortOutput(aggregationOutput);
  metrics_.testValue = std::get<0>(cohortOutput).at(0);
  metrics_.controlValue = std::get<0>(cohortOutput).at(1);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testValue = std::get<1>(cohortOutput).at(i);
    cohortMetrics_[i].controlValue = std::get<2>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumReachedValues() {
  XLOG(INFO) << "Aggregate reachedValues";
  // Aggregate across test/control and cohorts
  std::vector<std::vector<std::vector<bool>>> valueSharesArray;
  for (auto input : attributor_->getReachedValues()) {
    auto valueShares = input.extractIntShare().getBooleanShares();
    valueSharesArray.push_back(std::move(valueShares));
  }
  auto oram =
      testCohortSignedWriteOnlyOramFactory_->create(numTestCohortGroups_);
  auto aggregationOutput = aggregate<true, valueWidth, true>(
      testCohortIndexShares_,
      valueSharesArray,
      numTestCohortGroups_,
      std::move(oram));

  // Extract metrics
  auto cohortOutput = revealTestCohortOutput(aggregationOutput);
  metrics_.reachedValue = std::get<0>(cohortOutput);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].reachedValue = std::get<1>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumValueSquared() {
  XLOG(INFO) << "Aggregate valueSquared";
  // Aggregate across test/control and cohorts
  auto valueShares =
      attributor_->getValueSquared().extractIntShare().getBooleanShares();
  auto oram = valueSquaredWriteOnlyOramFactory_->create(numCohortGroups_);
  auto aggregationOutput = aggregate<false, valueSquaredWidth, false>(
      cohortIndexShares_, valueShares, numCohortGroups_, std::move(oram));

  // Extract metrics
  auto cohortOutput = revealCohortOutput(aggregationOutput);
  metrics_.testValueSquared = std::get<0>(cohortOutput).at(0);
  metrics_.controlValueSquared = std::get<0>(cohortOutput).at(1);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testValueSquared = std::get<1>(cohortOutput).at(i);
    cohortMetrics_[i].controlValueSquared = std::get<2>(cohortOutput).at(i);
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

template <int schedulerId>
template <bool isSigned, int8_t width>
std::tuple<
    NativeIntp<isSigned, width>,
    std::vector<NativeIntp<isSigned, width>>>
Aggregator<schedulerId>::revealTestCohortOutput(
    std::vector<SecInt<schedulerId, isSigned, width>> aggregationOutput) const {
  // Extract metrics
  std::vector<NativeIntp<isSigned, width>> testCohortOutput;
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    testCohortOutput.push_back(
        aggregationOutput.at(i).extractIntShare().getValue());
  }

  // Initialize test metrics for the case where there are no partner cohorts
  auto test = aggregationOutput.at(0);
  for (size_t i = 1; i < numPartnerCohorts_; ++i) {
    // Compute test metrics by summing by cohort metrics
    test = test + aggregationOutput.at(i);
  }

  return std::make_tuple(test.extractIntShare().getValue(), testCohortOutput);
}

} // namespace private_lift
