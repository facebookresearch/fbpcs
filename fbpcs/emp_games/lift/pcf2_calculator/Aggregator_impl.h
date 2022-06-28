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
  numGroups_ = std::max(2 * numPartnerCohorts_, uint32_t(2));
  numTestGroups_ = std::max(numPartnerCohorts_ + 1, uint32_t(2));

  if (numGroups_ > 4) {
    // If the ORAM size is larger than 4, linear ORAM is less efficient
    // theoretically
    unsignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<false, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
    signedWriteOnlyOramFactory_ =
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
    unsignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<false, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
    signedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<true, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
    valueSquaredWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<false, valueSquaredWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
  }

  if (numTestGroups_ > 4) {
    testUnsignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<false, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
    testSignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<true, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
  } else {
    testUnsignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<false, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
    testSignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<true, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
  }
}

template <int schedulerId>
std::string Aggregator<schedulerId>::toJson() const {
  GroupedLiftMetrics groupedLiftMetrics;

  /*
   * Rationale for getting max key instead of using umap.size():
   *  If the dataset does not record a row for a given cohort_id,
   *  we would get out of bound exception.
   */
  auto getMaxKey =
      [](const std::unordered_map<int64_t, OutputMetricsData>& _map) {
        int64_t max = 0;
        for (auto kv : _map) {
          max = kv.first > max ? kv.first : max;
        }
        return max;
      };
  if (!cohortMetrics_.empty()) {
    groupedLiftMetrics.cohortMetrics.resize(getMaxKey(cohortMetrics_) + 1);
  } else {
    groupedLiftMetrics.cohortMetrics.clear();
  }
  if (!publisherBreakdowns_.empty()) {
    groupedLiftMetrics.publisherBreakdowns.resize(
        getMaxKey(publisherBreakdowns_) + 1);
  } else {
    groupedLiftMetrics.publisherBreakdowns.clear();
  }
  groupedLiftMetrics.reset();

  groupedLiftMetrics.metrics = metrics_.toLiftMetrics();
  for (auto kv : cohortMetrics_) {
    groupedLiftMetrics.cohortMetrics[kv.first] = kv.second.toLiftMetrics();
  }

  for (auto kv : publisherBreakdowns_) {
    groupedLiftMetrics.publisherBreakdowns[kv.first] =
        kv.second.toLiftMetrics();
  }

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
  auto oram = unsignedWriteOnlyOramFactory_->create(numGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, true>(
      indexShares_, valueSharesArray, numGroups_, std::move(oram));

  // Extract metrics
  auto populationOutput = revealPopulationOutput(aggregationOutput, false);
  metrics_.testEvents = std::get<0>(populationOutput);
  metrics_.controlEvents = std::get<1>(populationOutput);
  auto cohortOutput = revealCohortOutput(aggregationOutput, false);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testEvents = std::get<0>(cohortOutput).at(i);
    cohortMetrics_[i].controlEvents = std::get<1>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumConverters() {
  XLOG(INFO) << "Aggregate converters";
  // Aggregate across test/control and cohorts
  std::vector<std::vector<bool>> valueShares(
      valueWidth, std::vector<bool>(numRows_, 0));
  valueShares[0] = attributor_->getConverters().extractBit().getValue();
  auto oram = unsignedWriteOnlyOramFactory_->create(numGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, false>(
      indexShares_, valueShares, numGroups_, std::move(oram));

  // Extract metrics
  auto populationOutput = revealPopulationOutput(aggregationOutput, false);
  metrics_.testConverters = std::get<0>(populationOutput);
  metrics_.controlConverters = std::get<1>(populationOutput);
  auto cohortOutput = revealCohortOutput(aggregationOutput, false);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testConverters = std::get<0>(cohortOutput).at(i);
    cohortMetrics_[i].controlConverters = std::get<1>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumNumConvSquared() {
  XLOG(INFO) << "Aggregate numConvSquared";
  // Aggregate across test/control and cohorts
  auto valueShares =
      attributor_->getNumConvSquared().extractIntShare().getBooleanShares();
  auto oram = unsignedWriteOnlyOramFactory_->create(numGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, false>(
      indexShares_, valueShares, numGroups_, std::move(oram));

  // Extract metrics
  auto populationOutput = revealPopulationOutput(aggregationOutput, false);
  metrics_.testNumConvSquared = std::get<0>(populationOutput);
  metrics_.controlNumConvSquared = std::get<1>(populationOutput);
  auto cohortOutput = revealCohortOutput(aggregationOutput, false);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testNumConvSquared = std::get<0>(cohortOutput).at(i);
    cohortMetrics_[i].controlNumConvSquared = std::get<1>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumMatch() {
  XLOG(INFO) << "Aggregate matchCount";
  // Aggregate across test/control and cohorts
  std::vector<std::vector<bool>> valueShares(
      valueWidth, std::vector<bool>(numRows_, 0));
  valueShares[0] = attributor_->getMatch().extractBit().getValue();
  auto oram = unsignedWriteOnlyOramFactory_->create(numGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, false>(
      indexShares_, valueShares, numGroups_, std::move(oram));

  // Extract metrics
  auto populationOutput = revealPopulationOutput(aggregationOutput, false);
  metrics_.testMatchCount = std::get<0>(populationOutput);
  metrics_.controlMatchCount = std::get<1>(populationOutput);
  auto cohortOutput = revealCohortOutput(aggregationOutput, false);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testMatchCount = std::get<0>(cohortOutput).at(i);
    cohortMetrics_[i].controlMatchCount = std::get<1>(cohortOutput).at(i);
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
  auto oram = testUnsignedWriteOnlyOramFactory_->create(numTestGroups_);
  auto aggregationOutput = aggregate<false, valueWidth, true>(
      testIndexShares_, valueSharesArray, numTestGroups_, std::move(oram));

  // Extract metrics
  auto populationOutput = revealPopulationOutput(aggregationOutput, true);
  metrics_.reachedConversions = std::get<0>(populationOutput);
  auto cohortOutput = revealCohortOutput(aggregationOutput, true);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].reachedConversions = std::get<0>(cohortOutput).at(i);
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
  auto oram = signedWriteOnlyOramFactory_->create(numGroups_);
  auto aggregationOutput = aggregate<true, valueWidth, true>(
      indexShares_, valueSharesArray, numGroups_, std::move(oram));

  // Extract metrics
  auto populationOutput = revealPopulationOutput(aggregationOutput, false);
  metrics_.testValue = std::get<0>(populationOutput);
  metrics_.controlValue = std::get<1>(populationOutput);
  auto cohortOutput = revealCohortOutput(aggregationOutput, false);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testValue = std::get<0>(cohortOutput).at(i);
    cohortMetrics_[i].controlValue = std::get<1>(cohortOutput).at(i);
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
  auto oram = testSignedWriteOnlyOramFactory_->create(numTestGroups_);
  auto aggregationOutput = aggregate<true, valueWidth, true>(
      testIndexShares_, valueSharesArray, numTestGroups_, std::move(oram));

  // Extract metrics
  auto populationOutput = revealPopulationOutput(aggregationOutput, true);
  metrics_.reachedValue = std::get<0>(populationOutput);
  auto cohortOutput = revealCohortOutput(aggregationOutput, true);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].reachedValue = std::get<0>(cohortOutput).at(i);
  }
}

template <int schedulerId>
void Aggregator<schedulerId>::sumValueSquared() {
  XLOG(INFO) << "Aggregate valueSquared";
  // Aggregate across test/control and cohorts
  auto valueShares =
      attributor_->getValueSquared().extractIntShare().getBooleanShares();
  auto oram = valueSquaredWriteOnlyOramFactory_->create(numGroups_);
  auto aggregationOutput = aggregate<false, valueSquaredWidth, false>(
      indexShares_, valueShares, numGroups_, std::move(oram));

  // Extract metrics
  auto populationOutput = revealPopulationOutput(aggregationOutput, false);
  metrics_.testValueSquared = std::get<0>(populationOutput);
  metrics_.controlValueSquared = std::get<1>(populationOutput);
  auto cohortOutput = revealCohortOutput(aggregationOutput, false);
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    cohortMetrics_[i].testValueSquared = std::get<0>(cohortOutput).at(i);
    cohortMetrics_[i].controlValueSquared = std::get<1>(cohortOutput).at(i);
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
std::pair<
    std::vector<NativeIntp<isSigned, width>>,
    std::vector<NativeIntp<isSigned, width>>>
Aggregator<schedulerId>::revealCohortOutput(
    std::vector<SecInt<schedulerId, isSigned, width>> aggregationOutput,
    bool testOnly) const {
  std::vector<NativeIntp<isSigned, width>> testCohortOutput;
  std::vector<NativeIntp<isSigned, width>> controlCohortOutput;
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    // Extract cohort metrics
    testCohortOutput.push_back(
        aggregationOutput.at(i).extractIntShare().getValue());
    if (!testOnly) {
      controlCohortOutput.push_back(aggregationOutput.at(i + numPartnerCohorts_)
                                        .extractIntShare()
                                        .getValue());
    }
  }
  return std::make_pair(testCohortOutput, controlCohortOutput);
}

template <int schedulerId>
template <bool isSigned, int8_t width>
std::pair<NativeIntp<isSigned, width>, NativeIntp<isSigned, width>>
Aggregator<schedulerId>::revealPopulationOutput(
    std::vector<SecInt<schedulerId, isSigned, width>> aggregationOutput,
    bool testOnly) const {
  // Initialize test/control metrics for the case where there are no partner
  // cohorts
  auto test = aggregationOutput.at(0);
  auto control =
      aggregationOutput.at(std::max(uint32_t(1), numPartnerCohorts_));
  for (size_t i = 1; i < numPartnerCohorts_; ++i) {
    // Compute test/control metrics by summing up cohort metrics for each
    // population
    test = test + aggregationOutput.at(i);
    if (!testOnly) {
      control = control + aggregationOutput.at(i + numPartnerCohorts_);
    }
  }
  auto testOutput = test.extractIntShare().getValue();
  NativeIntp<isSigned, width> controlOutput;
  if (!testOnly) {
    controlOutput = control.extractIntShare().getValue();
  }
  return std::make_pair(testOutput, controlOutput);
}

} // namespace private_lift
