/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tuple>
#include <unordered_map>

#include "OutputMetricsData.h"
#include "folly/logging/xlog.h"

#include <fbpcf/mpc/EmpGame.h>
#include "../../common/Functional.h"
#include "../common/GroupedLiftMetrics.h"

namespace private_lift {

constexpr int32_t PUBLISHER = static_cast<int>(fbpcf::Party::Alice);
constexpr int32_t PARTNER = static_cast<int>(fbpcf::Party::Bob);
constexpr int32_t QUICK_BITS = 32;
constexpr int32_t FULL_BITS = 64;

template <int32_t MY_ROLE>
constexpr auto privatelyShareInt =
    private_measurement::secret_sharing::privatelyShareInt<MY_ROLE>;
template <int32_t MY_ROLE>
constexpr auto privatelyShareIntsFromPublisher =
    private_measurement::secret_sharing::privatelyShareIntsFromAlice<MY_ROLE>;
template <int32_t MY_ROLE>
constexpr auto privatelyShareIntsFromPartner =
    private_measurement::secret_sharing::privatelyShareIntsFromBob<MY_ROLE>;
template <int32_t MY_ROLE>
constexpr auto privatelyShareBitsFromPublisher =
    private_measurement::secret_sharing::privatelyShareBitsFromAlice<MY_ROLE>;
template <int32_t MY_ROLE>
constexpr auto privatelyShareBitsFromPartner =
    private_measurement::secret_sharing::privatelyShareBitsFromBob<MY_ROLE>;
template <int32_t MY_ROLE>
constexpr auto privatelyShareIntArraysFromPartner = private_measurement::
    secret_sharing::privatelyShareIntArraysNoPaddingFromBob<MY_ROLE>;

template <int32_t MY_ROLE>
template <class T>
T OutputMetrics<MY_ROLE>::reveal(const emp::Integer& empInteger) const {
  return shouldUseXorEncryption() ? empInteger.reveal<T>(emp::XOR)
                                  : empInteger.reveal<T>();
}

template <int32_t MY_ROLE>
std::string OutputMetrics<MY_ROLE>::playGame() {
  validateNumRows();
  initNumGroups();
  initShouldSkipValues();
  initBitsForValues();
  calculateAll();

  // Print the outputs
  XLOG(INFO) << "\nEMP Output (Role=" << MY_ROLE << "):\n" << metrics_;

  // Print each cohort header. Note that the publisher won't know anything
  // about the group header (only a generic index for which group we are
  // currently outputting.
  for (size_t i = 0; i < cohortMetrics_.size(); ++i) {
    XLOG(INFO) << "\ncohort [" << i << "] results:";
    if (MY_ROLE == PARTNER) {
      // This section only applies if features were suppled instead of cohorts
      if (inputData_.getGroupIdToFeatures().size() > 0) {
        auto features = inputData_.getGroupIdToFeatures().at(i);
        std::stringstream headerSs;
        for (size_t j = 0; j < features.size(); ++j) {
          auto featureHeader = inputData_.getFeatureHeader().at(j);
          headerSs << featureHeader << "=" << features.at(j);
          if (j + 1 < features.size()) {
            headerSs << ", ";
          }
        }
        XLOG(INFO) << headerSs.str();
      }
    } else {
      XLOG(INFO) << "(Feature header unknown to publisher)";
    }

    auto cohortMetrics = cohortMetrics_[i];
    XLOG(INFO) << cohortMetrics;
  }
  return toJson();
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::writeOutputToFile(std::ostream& outfile) {
  // Start by outputting the overall results
  outfile << "Overall"
          << ",";
  outfile << metrics_.testEvents << ",";
  outfile << metrics_.controlEvents << ",";
  // Value metrics are only relevant for conversion lift
  if (inputData_.getLiftGranularityType() ==
      InputData::LiftGranularityType::Conversion) {
    outfile << metrics_.testValue << ",";
    outfile << metrics_.controlValue << ",";
    outfile << metrics_.testValueSquared << ",";
    outfile << metrics_.controlValueSquared << ",";
    outfile << metrics_.testNumConvSquared << ",";
    outfile << metrics_.controlNumConvSquared << ",";
  }
  outfile << metrics_.testMatchCount << ",";
  outfile << metrics_.controlMatchCount << "\n";

  // Then output results for each group
  // Print each cohort header. Note that the publisher won't know anything
  // about the group header (only a generic index for which group we are
  // currently outputting.
  for (size_t i = 0; i < cohortMetrics_.size(); ++i) {
    auto subOut = cohortMetrics_.at(i);
    if (MY_ROLE == PARTNER) {
      auto features = inputData_.getGroupIdToFeatures().at(i);
      for (auto j = 0; j < features.size(); ++j) {
        auto featureHeader = inputData_.getFeatureHeader().at(j);
        outfile << featureHeader << "=" << features.at(j);
        if (j + 1 < features.size()) {
          outfile << " AND ";
        }
      }
      outfile << ",";
    } else {
      outfile << "cohort " << i << ",";
    }

    outfile << subOut.testEvents << ",";
    outfile << subOut.controlEvents << ",";
    outfile << subOut.testConverters << ",";
    outfile << subOut.controlConverters << ",";
    // Value metrics are only relevant for conversion lift
    if (inputData_.getLiftGranularityType() ==
        InputData::LiftGranularityType::Conversion) {
      outfile << subOut.testValue << ",";
      outfile << subOut.controlValue << ",";
      outfile << subOut.testValueSquared << ",";
      outfile << subOut.controlValueSquared << ",";
      outfile << subOut.testNumConvSquared << ",";
      outfile << subOut.controlNumConvSquared << ",";
    }
  }
}

template <int32_t MY_ROLE>
std::string OutputMetrics<MY_ROLE>::toJson() const {
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
  if (cohortMetrics_.size() > 0) {
    groupedLiftMetrics.cohortMetrics.resize(getMaxKey(cohortMetrics_) + 1);
  } else {
    groupedLiftMetrics.cohortMetrics.clear();
  }
  if (publisherBreakdowns_.size() > 0) {
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

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::validateNumRows() {
  // TODO: We shouldn't be using MPC for this, it should just be shared over
  // a normal network socket as part of the protocol setup
  auto numRows = privatelyShareInt<MY_ROLE>(n_);
  auto publisherNumRows = numRows.publisherInt().template reveal<int64_t>();
  auto partnerNumRows = numRows.partnerInt().template reveal<int64_t>();

  if (publisherNumRows != partnerNumRows) {
    // Using LOG(FATAL) will make the publisher hang since they'll never get the
    // reveal for some reason.
    XLOG(ERR) << "The publisher has " << publisherNumRows
              << " rows in their input, while the partner has "
              << partnerNumRows << " rows.";
    exit(1);
  }
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::initNumGroups() {
  // TODO: We shouldn't be using MPC for this, it should just be shared over
  // a normal network socket as part of the protocol setup
  XLOG(INFO) << "Set up number of partner groups";
  // emp::Integer operates on int64_t values, so we do a static cast here
  // This is fine since we shouldn't be handling more than 2^63-1 groups...
  auto numGroups = privatelyShareInt<MY_ROLE>(inputData_.getNumGroups());
  numPublisherBreakdowns_ = numGroups.publisherInt().template reveal<int64_t>();
  numPartnerCohorts_ = numGroups.partnerInt().template reveal<int64_t>();

  // We pre-share the bitmasks for each group since they will be used
  // multiple times throughout the computation
  for (size_t i = 0; i < numPublisherBreakdowns_; ++i) {
    publisherBitmasks_[i] =
        privatelyShareBitsFromPublisher<MY_ROLE>(inputData_.bitmaskFor(i), n_);
  }

  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    partnerBitmasks_[i] =
        privatelyShareBitsFromPartner<MY_ROLE>(inputData_.bitmaskFor(i), n_);
  }
  XLOG(INFO) << "Will be computing metrics for " << numPublisherBreakdowns_
             << " publisher breakdowns and " << numPartnerCohorts_
             << " partner cohorts";
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::initShouldSkipValues() {
  XLOG(INFO) << "Determine if value-based calculations should be skipped";
  bool hasValues = inputData_.getPurchaseValueArrays().empty();
  emp::Bit hasValuesBit{hasValues, PARTNER};
  shouldSkipValues_ = hasValuesBit.reveal<bool>();
  XLOG(INFO) << "shouldSkipValues = " << shouldSkipValues_;
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::initBitsForValues() {
  if (!shouldSkipValues_) {
    XLOG(INFO) << "Set up number of bits needed for purchase value sharing";
    auto valueBits = static_cast<int64_t>(inputData_.getNumBitsForValue());
    auto valueSquaredBits =
        static_cast<int64_t>(inputData_.getNumBitsForValueSquared());
    emp::Integer valueBitsInteger{
        private_measurement::INT_SIZE, valueBits, PARTNER};
    emp::Integer valueSquaredBitsInteger{
        private_measurement::INT_SIZE, valueSquaredBits, PARTNER};
    // TODO: Figure out why this isn't working when using values other than
    // 32/64
    valueBits_ = valueBitsInteger.reveal<int64_t>() <= QUICK_BITS ? QUICK_BITS
                                                                  : FULL_BITS;
    valueSquaredBits_ = valueSquaredBitsInteger.reveal<int64_t>() <= QUICK_BITS
        ? QUICK_BITS
        : FULL_BITS;
    XLOG(INFO) << "Num bits for values: " << valueBits_;
    XLOG(INFO) << "Num bits for values squared: " << valueSquaredBits_;
  }
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::calculateAll() {
  XLOG(INFO) << "Start calculation of output metrics";

  std::vector<std::vector<emp::Integer>> purchaseValueArrays;

  if (!shouldSkipValues_) {
    XLOG(INFO) << "Share purchase values";
    purchaseValueArrays = privatelyShareIntArraysFromPartner<MY_ROLE>(
        inputData_.getPurchaseValueArrays(),
        n_, /* numVals */
        numConversionsPerUser_ /* arraySize */,
        valueBits_ /* bitLen */);
  }

  auto validPurchaseArrays = calculateValidPurchases();

  std::vector<std::vector<emp::Integer>> purchaseValueSquaredArrays;

  // If this is (value-based) conversion lift, we also need to share purchase
  // values squared
  if (!shouldSkipValues_ &&
      inputData_.getLiftGranularityType() ==
          InputData::LiftGranularityType::Conversion) {
    purchaseValueSquaredArrays = privatelyShareIntArraysFromPartner<MY_ROLE>(
        inputData_.getPurchaseValueSquaredArrays(),
        n_, /* numVals */
        numConversionsPerUser_ /* arraySize */,
        valueSquaredBits_ /* bitLen */);
  }

  calculateStatistics(
      GroupType::TEST,
      purchaseValueArrays,
      purchaseValueSquaredArrays,
      validPurchaseArrays);
  calculateStatistics(
      GroupType::CONTROL,
      purchaseValueArrays,
      purchaseValueSquaredArrays,
      validPurchaseArrays);
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::calculateStatistics(
    const OutputMetrics::GroupType& groupType,
    const std::vector<std::vector<emp::Integer>>& purchaseValueArrays,
    const std::vector<std::vector<emp::Integer>>& purchaseValueSquaredArrays,
    const std::vector<std::vector<emp::Bit>>& validPurchaseArrays) {
  XLOG(INFO) << "Calculate " << getGroupTypeStr(groupType)
             << " events, value, and value squared";
  auto bits = calculatePopulation(
      groupType,
      groupType == GroupType::TEST ? inputData_.getTestPopulation()
                                   : inputData_.getControlPopulation());
  auto eventArrays = calculateEvents(groupType, bits, validPurchaseArrays);
  std::vector<emp::Bit> reachedArray;
  calculateMatchCount(groupType, bits, purchaseValueArrays);
  if (groupType == GroupType::TEST) {
    reachedArray = calculateImpressions(groupType, bits);
    calculateReachedConversions(groupType, validPurchaseArrays, reachedArray);
  }

  // If this is (value-based) conversion lift, calculate value metrics now
  if (!shouldSkipValues_ &&
      inputData_.getLiftGranularityType() ==
          InputData::LiftGranularityType::Conversion) {
    calculateValue(groupType, purchaseValueArrays, eventArrays, reachedArray);
    calculateValueSquared(groupType, purchaseValueSquaredArrays, eventArrays);
  }
}

template <int32_t MY_ROLE>
std::vector<emp::Bit> OutputMetrics<MY_ROLE>::calculatePopulation(
    const OutputMetrics::GroupType& groupType,
    const std::vector<int64_t> populationVec) {
  XLOG(INFO) << "Calculate " << getGroupTypeStr(groupType) << " population";
  const std::vector<emp::Bit> populationBits =
      privatelyShareBitsFromPublisher<MY_ROLE>(populationVec, n_);
  return populationBits;
}

template <int32_t MY_ROLE>
std::vector<std::vector<emp::Bit>>
OutputMetrics<MY_ROLE>::calculateValidPurchases() {
  // TODO: We're using 32 bits for timestamps along with an offset setting the
  // epoch to 2019-01-01. This will break in the year 2087.
  XLOG(INFO) << "Share opportunity timestamps";
  const std::vector<emp::Integer> opportunityTimestamps =
      privatelyShareIntsFromPublisher<MY_ROLE>(
          inputData_.getOpportunityTimestamps(), n_, QUICK_BITS);
  XLOG(INFO) << "Share purchase timestamps";
  const std::vector<std::vector<emp::Integer>> purchaseTimestampArrays =
      privatelyShareIntArraysFromPartner<MY_ROLE>(
          inputData_.getPurchaseTimestampArrays(),
          n_, /* numVals */
          numConversionsPerUser_ /* arraySize */,
          QUICK_BITS /* bitLen */);

  XLOG(INFO) << "Calculate valid purchases";
  return private_measurement::functional::zip_apply(
      [](emp::Integer oppTs,
         std::vector<emp::Integer> purchaseTsArray) -> std::vector<emp::Bit> {
        std::vector<emp::Bit> vec;
        for (const auto& purchaseTs : purchaseTsArray) {
          const emp::Integer ten{
              static_cast<int>(purchaseTs.size()), 10, emp::PUBLIC};
          vec.push_back(purchaseTs + ten > oppTs);
        }
        return vec;
      },
      opportunityTimestamps.begin(),
      opportunityTimestamps.end(),
      purchaseTimestampArrays.begin());
}

template <int32_t MY_ROLE>
std::vector<std::vector<emp::Bit>> OutputMetrics<MY_ROLE>::calculateEvents(
    const OutputMetrics::GroupType& groupType,
    const std::vector<emp::Bit>& populationBits,
    const std::vector<std::vector<emp::Bit>>& validPurchaseArrays) {
  XLOG(INFO) << "Calculate " << getGroupTypeStr(groupType)
             << " conversions & converters";

  // We are going to cleverly transpose this so we won't have to repeatedly
  // do so below. vector[i] contains the histogram bitmask for bin i. This is in
  // contrast to a "normal" return type for zip_and_map which would be output as
  // a row-based vector of events. By transposing this ahead of time, we can
  // use utilities like `sum` defined below.
  std::vector<std::vector<emp::Bit>> convHistograms;
  if (validPurchaseArrays.size() > 0) {
    for (size_t i = 0; i <= validPurchaseArrays.at(0).size(); ++i) {
      convHistograms.emplace_back();
    }
  }

  // This code needs cleaned up. It's getting hard to understand.
  // There's not a more advanced overload of zip_and_map, so we are now relying
  // upon side-effects of the lambda. A later diff in this stack will attempt
  // to nuke this and fix it properly.
  auto [eventArrays, converterArrays, squaredNumConvs] =
      private_measurement::secret_sharing::zip_and_map<
          emp::Bit,
          std::vector<emp::Bit>,
          std::vector<emp::Bit>,
          emp::Bit,
          emp::Integer>(
          populationBits,
          validPurchaseArrays,
          [&convHistograms](
              emp::Bit isUser, std::vector<emp::Bit> validPurchaseArray)
              -> std::tuple<std::vector<emp::Bit>, emp::Bit, emp::Integer> {
            std::vector<emp::Bit> vec;
            emp::Integer numConvSquared{
                private_measurement::INT_SIZE, 0, emp::PUBLIC};
            emp::Bit anyValidPurchase{false, emp::PUBLIC};

            for (size_t i = 0; i < validPurchaseArray.size(); ++i) {
              auto cond = isUser & validPurchaseArray.at(i);
              auto newPurchase = cond & !anyValidPurchase;
              vec.push_back(cond);
              // If this event is valid and we haven't taken the accumulation
              // yet, use this value as the sumSquared accumulation. The number
              // of valid events if this event is valid is the remaining number
              // of elements in the array
              auto numConv = validPurchaseArray.size() - i;
              auto convSquared = static_cast<int64_t>(numConv * numConv);
              emp::Integer numConvSquaredIfValid{
                  static_cast<int>(numConvSquared.size()),
                  convSquared,
                  emp::PUBLIC};
              numConvSquared =
                  emp::If(newPurchase, numConvSquaredIfValid, numConvSquared);

              // Interpretation: at index `i`, we're detecting if we should
              // increment the histogram at value `size() - i` because it means
              // the user had `size() - i` *valid* conversions. So we set
              // `convHistograms[numConv][_]` to true if this is the first valid
              // purchase we have seen. It's a bit backwards from the above
              // logic since we're not updating index `i` here, but it's
              // unfortunately the simplest way to update the histogram. The
              // alternative is to iterate another loop, which would be quite
              // expensive to run in practice.
              convHistograms[numConv].push_back(newPurchase);
              anyValidPurchase = anyValidPurchase | cond;
            }
            // If the person *never* converted, increment the zero bucket now
            // Note that the isUser check is very important to avoid
            // overcounting
            convHistograms[0].push_back(isUser & !anyValidPurchase);
            return std::make_tuple(vec, anyValidPurchase, numConvSquared);
          });

  if (groupType == GroupType::TEST) {
    metrics_.testEvents = sum(eventArrays);
    metrics_.testConverters = sum(converterArrays);
    metrics_.testNumConvSquared = sum(squaredNumConvs);
    // TODO: Computational shortcut possible by recognizing that bin 0
    // is equivalent to population - sum(convs in other bins).
    // We can avoid a relatively expensive bit sum.
    for (size_t bin = 0; bin < convHistograms.size(); ++bin) {
      metrics_.testConvHistogram.push_back(sum(convHistograms.at(bin)));
    }
  } else {
    metrics_.controlEvents = sum(eventArrays);
    metrics_.controlConverters = sum(converterArrays);
    metrics_.controlNumConvSquared = sum(squaredNumConvs);
    for (size_t bin = 0; bin < convHistograms.size(); ++bin) {
      metrics_.controlConvHistogram.push_back(sum(convHistograms.at(bin)));
    }
  }

  // And compute for breakdowns + cohorts
  // TODO: These could be abstracted into a common function
  for (size_t i = 0; i < numPublisherBreakdowns_; ++i) {
    const auto& mask = publisherBitmasks_.at(i);
    auto groupEventBits =
        private_measurement::secret_sharing::multiplyBitmask(eventArrays, mask);
    auto groupConverterBits =
        private_measurement::secret_sharing::multiplyBitmask(
            converterArrays, mask);
    auto groupEvents = sum(groupEventBits);
    auto groupConverters = sum(groupConverterBits);
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        squaredNumConvs, mask);

    std::vector<int64_t> groupConvHistogram;
    for (size_t bin = 0; bin < convHistograms.size(); ++bin) {
      auto binBits = private_measurement::secret_sharing::multiplyBitmask(
          convHistograms.at(bin), mask);
      groupConvHistogram.push_back(sum(binBits));
    }

    if (groupType == GroupType::TEST) {
      publisherBreakdowns_[i].testEvents = groupEvents;
      publisherBreakdowns_[i].testConverters = groupConverters;
      publisherBreakdowns_[i].testNumConvSquared = sum(groupInts);
      publisherBreakdowns_[i].testConvHistogram = std::move(groupConvHistogram);
    } else {
      publisherBreakdowns_[i].controlEvents = groupEvents;
      publisherBreakdowns_[i].controlConverters = groupConverters;
      publisherBreakdowns_[i].controlNumConvSquared = sum(groupInts);
      publisherBreakdowns_[i].controlConvHistogram =
          std::move(groupConvHistogram);
    }
  }

  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    const auto& mask = partnerBitmasks_.at(i);
    auto groupEventBits =
        private_measurement::secret_sharing::multiplyBitmask(eventArrays, mask);
    auto groupConverterBits =
        private_measurement::secret_sharing::multiplyBitmask(
            converterArrays, mask);
    auto groupEvents = sum(groupEventBits);
    auto groupConverters = sum(groupConverterBits);
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        squaredNumConvs, mask);

    std::vector<int64_t> groupConvHistogram;
    for (size_t bin = 0; bin < convHistograms.size(); ++bin) {
      auto binBits = private_measurement::secret_sharing::multiplyBitmask(
          convHistograms.at(bin), mask);
      groupConvHistogram.push_back(sum(binBits));
    }

    if (groupType == GroupType::TEST) {
      cohortMetrics_[i].testEvents = groupEvents;
      cohortMetrics_[i].testConverters = groupConverters;
      cohortMetrics_[i].testNumConvSquared = sum(groupInts);
      cohortMetrics_[i].testConvHistogram = std::move(groupConvHistogram);
    } else {
      cohortMetrics_[i].controlEvents = groupEvents;
      cohortMetrics_[i].controlConverters = groupConverters;
      cohortMetrics_[i].controlNumConvSquared = sum(groupInts);
      cohortMetrics_[i].controlConvHistogram = std::move(groupConvHistogram);
    }
  }
  return eventArrays;
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::calculateMatchCount(
    const OutputMetrics::GroupType& groupType,
    const std::vector<emp::Bit>& populationBits,
    const std::vector<std::vector<emp::Integer>>& purchaseValueArrays) {
  XLOG(INFO) << "Calculate " << getGroupTypeStr(groupType) << " MatchCount";
  // a valid test/control match is when a person with an opportunity who made
  // ANY nonzero conversion. Therefore we can just check first if an opportunity
  // is valid, then bitwise AND this with the bitwise OR over all purchases (to
  // check for purchases). This gets us a binary indication if a user is
  // matched with any opportunity

  XLOG(INFO) << "Share opportunity timestamps";
  const std::vector<emp::Integer> opportunityTimestamps =
      privatelyShareIntsFromPublisher<MY_ROLE>(
          inputData_.getOpportunityTimestamps(), n_, QUICK_BITS);
  XLOG(INFO) << "Share purchase timestamps";
  const std::vector<std::vector<emp::Integer>> purchaseTimestampArrays =
      privatelyShareIntArraysFromPartner<MY_ROLE>(
          inputData_.getPurchaseTimestampArrays(),
          n_, /* numVals */
          numConversionsPerUser_ /* arraySize */,
          QUICK_BITS /* bitLen */);
  auto matchArrays = private_measurement::functional::zip_apply(
      [](emp::Bit isUser,
         emp::Integer opportunityTimestamp,
         std::vector<emp::Integer> purchaseTimestampArray) -> emp::Bit {
        const emp::Integer zero = emp::Integer{
            static_cast<int>(opportunityTimestamp.size()), 0, emp::PUBLIC};
        emp::Bit validOpportunity =
            (isUser &
             (opportunityTimestamp > zero)); // check if opportunity is valid
        emp::Bit isUserMatched = emp::Bit{0, emp::PUBLIC};
        for (const auto& purchaseTS : purchaseTimestampArray) {
          // check for the existence of a valid purchase
          isUserMatched = isUserMatched | (purchaseTS > zero);
        }
        return isUserMatched & validOpportunity;
      },
      populationBits.begin(),
      populationBits.end(),
      opportunityTimestamps.begin(),
      purchaseTimestampArrays.begin());

  if (groupType == GroupType::TEST) {
    metrics_.testMatchCount = sum(matchArrays);
  } else {
    metrics_.controlMatchCount = sum(matchArrays);
  }

  // And compute for breakdowns + cohorts
  // TODO: These could be abstracted into a common function
  for (size_t i = 0; i < numPublisherBreakdowns_; ++i) {
    auto groupBits = private_measurement::secret_sharing::multiplyBitmask(
        matchArrays, publisherBitmasks_.at(i));
    if (groupType == GroupType::TEST) {
      publisherBreakdowns_[i].testMatchCount = sum(groupBits);
    } else {
      publisherBreakdowns_[i].controlMatchCount = sum(groupBits);
    }
  }

  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    auto groupBits = private_measurement::secret_sharing::multiplyBitmask(
        matchArrays, partnerBitmasks_.at(i));
    if (groupType == GroupType::TEST) {
      cohortMetrics_[i].testMatchCount = sum(groupBits);
    } else {
      cohortMetrics_[i].controlMatchCount = sum(groupBits);
    }
  }
}

template <int32_t MY_ROLE>
std::vector<emp::Bit> OutputMetrics<MY_ROLE>::calculateImpressions(
    const OutputMetrics::GroupType& groupType,
    const std::vector<emp::Bit>& populationBits) {
  XLOG(INFO) << "Calculate " << getGroupTypeStr(groupType)
             << " impressions & reach";

  const std::vector<emp::Integer> numImpressions =
      privatelyShareIntsFromPublisher<MY_ROLE>(
          inputData_.getNumImpressions(), n_, FULL_BITS);

  auto [impressionsArray, reachArray] = private_measurement::secret_sharing::
      zip_and_map<emp::Bit, emp::Integer, emp::Integer, emp::Bit>(
          populationBits,
          numImpressions,
          [](emp::Bit isUser,
             emp::Integer numImpressions) -> std::pair<emp::Integer, emp::Bit> {
            const emp::Integer zero =
                emp::Integer{private_measurement::INT_SIZE, 0, emp::PUBLIC};
            return std::make_pair(
                emp::If(isUser, numImpressions, zero),
                isUser & (numImpressions > zero));
          });

  // And compute for breakdowns + cohorts
  // TODO: These could be abstracted into a common function
  for (size_t i = 0; i < numPublisherBreakdowns_; ++i) {
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        impressionsArray, publisherBitmasks_.at(i));
    auto groupBits = private_measurement::secret_sharing::multiplyBitmask(
        reachArray, publisherBitmasks_.at(i));
  }

  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        impressionsArray, partnerBitmasks_.at(i));
    auto groupBits = private_measurement::secret_sharing::multiplyBitmask(
        reachArray, partnerBitmasks_.at(i));
  }

  return reachArray;
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::calculateReachedConversions(
    const OutputMetrics::GroupType& groupType,
    const std::vector<std::vector<emp::Bit>>& validPurchaseArrays,
    const std::vector<emp::Bit>& reachedArray) {
  XLOG(INFO) << "Calculate " << getGroupTypeStr(groupType)
             << " reached conversions";
  if (groupType != GroupType::TEST) {
    XLOG(FATAL)
        << "Calculation of reached conversions for control group not supported";
  }

  std::vector<std::vector<emp::Bit>> reachedConversions =
      private_measurement::functional::zip_apply(
          [](std::vector<emp::Bit> validPurchases,
             emp::Bit reached) -> std::vector<emp::Bit> {
            std::vector<emp::Bit> res;
            for (const auto& validPurchase : validPurchases) {
              res.emplace_back(validPurchase & reached);
            }
            return res;
          },
          validPurchaseArrays.begin(),
          validPurchaseArrays.end(),
          reachedArray.begin());

  if (groupType == GroupType::TEST) {
    metrics_.reachedConversions = sum(reachedConversions);
  } else {
    XLOG(FATAL)
        << "Calculation of reached conversions for control group not supported";
  }

  // And compute for breakdowns + cohorts
  // TODO: These could be abstracted into a common function
  for (size_t i = 0; i < numPublisherBreakdowns_; ++i) {
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        reachedConversions, publisherBitmasks_.at(i));
    if (groupType == GroupType::TEST) {
      publisherBreakdowns_[i].reachedConversions = sum(groupInts);
    } else {
      XLOG(FATAL) << "Calculation of reached conversions for control group not "
                     "supported";
    }
  }

  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        reachedConversions, partnerBitmasks_.at(i));
    if (groupType == GroupType::TEST) {
      cohortMetrics_[i].reachedConversions = sum(groupInts);
    } else {
      XLOG(FATAL) << "Calculation of reached conversions for control group not "
                     "supported";
    }
  }
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::calculateValue(
    const OutputMetrics::GroupType& groupType,
    const std::vector<std::vector<emp::Integer>>& purchaseValueArrays,
    const std::vector<std::vector<emp::Bit>>& eventArrays,
    const std::vector<emp::Bit>& reachedArray) {
  XLOG(INFO) << "Calculate " << getGroupTypeStr(groupType) << " value";
  std::vector<std::vector<emp::Integer>> valueArrays =
      private_measurement::functional::zip_apply(
          [](std::vector<emp::Bit> testEvents,
             std::vector<emp::Integer> purchaseValues)
              -> std::vector<emp::Integer> {
            std::vector<emp::Integer> vec;
            if (testEvents.size() != purchaseValues.size()) {
              XLOG(FATAL) << "Numbers of test event bits and/or purchase "
                             "values are inconsistent.";
            }
            for (size_t i = 0; i < testEvents.size(); ++i) {
              const emp::Integer zero = emp::Integer{
                  static_cast<int>(purchaseValues.at(i).size()),
                  0,
                  emp::PUBLIC};
              vec.emplace_back(
                  emp::If(testEvents.at(i), purchaseValues.at(i), zero));
            }
            return vec;
          },
          eventArrays.begin(),
          eventArrays.end(),
          purchaseValueArrays.begin());

  std::vector<std::vector<emp::Integer>> reachedValue;
  if (groupType == GroupType::TEST) {
    reachedValue = private_measurement::functional::zip_apply(
        [](std::vector<emp::Integer> validValues,
           emp::Bit reached) -> std::vector<emp::Integer> {
          std::vector<emp::Integer> vec;
          for (const auto& validValue : validValues) {
            const emp::Integer zero = emp::Integer{
                static_cast<int>(validValue.size()), 0, emp::PUBLIC};
            vec.emplace_back(emp::If(reached, validValue, zero));
          }
          return vec;
        },
        valueArrays.begin(),
        valueArrays.end(),
        reachedArray.begin());

    metrics_.testValue = sum(valueArrays);
    metrics_.reachedValue = sum(reachedValue);
  } else {
    metrics_.controlValue = sum(valueArrays);
  }

  // And compute for breakdowns + cohorts
  // TODO: These could be abstracted into a common function
  for (size_t i = 0; i < numPublisherBreakdowns_; ++i) {
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        valueArrays, publisherBitmasks_.at(i));
    if (groupType == GroupType::TEST) {
      publisherBreakdowns_[i].testValue = sum(groupInts);
      auto reachedGroupInts =
          private_measurement::secret_sharing::multiplyBitmask(
              reachedValue, publisherBitmasks_.at(i));
      publisherBreakdowns_[i].reachedValue = sum(reachedGroupInts);
    } else {
      publisherBreakdowns_[i].controlValue = sum(groupInts);
    }
  }

  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        valueArrays, partnerBitmasks_.at(i));
    if (groupType == GroupType::TEST) {
      cohortMetrics_[i].testValue = sum(groupInts);
      auto reachedGroupInts =
          private_measurement::secret_sharing::multiplyBitmask(
              reachedValue, partnerBitmasks_.at(i));
      cohortMetrics_[i].reachedValue = sum(reachedGroupInts);
    } else {
      cohortMetrics_[i].controlValue = sum(groupInts);
    }
  }
}

template <int32_t MY_ROLE>
void OutputMetrics<MY_ROLE>::calculateValueSquared(
    const OutputMetrics::GroupType& groupType,
    const std::vector<std::vector<emp::Integer>>& purchaseValueSquaredArrays,
    const std::vector<std::vector<emp::Bit>>& eventArrays) {
  XLOG(INFO) << "Calculate " << getGroupTypeStr(groupType) << " value squared";
  auto squaredValues = private_measurement::functional::zip_apply(
      [](std::vector<emp::Bit> events,
         std::vector<emp::Integer> purchaseValuesSquared) -> emp::Integer {
        emp::Integer sumSquared{
            static_cast<int>(purchaseValuesSquared.at(0).size()),
            0,
            emp::PUBLIC};
        if (events.size() != purchaseValuesSquared.size()) {
          XLOG(FATAL) << "Numbers of event bits and purchase values squared "
                         "are inconsistent.";
        }
        emp::Bit tookAccumulationAlready{false, emp::PUBLIC};
        for (size_t i = 0; i < events.size(); ++i) {
          // If this event is valid and we haven't taken the accumulation yet,
          // use this value as the sumSquared accumulation.
          // emp::If(condition, true_case, false_case)
          auto cond = events.at(i) & !tookAccumulationAlready;
          sumSquared = emp::If(cond, purchaseValuesSquared.at(i), sumSquared);
          // Always make sure we keep tookAccumulationAlready up-to-date
          tookAccumulationAlready = tookAccumulationAlready | events.at(i);
        }
        return sumSquared;
      },
      eventArrays.begin(),
      eventArrays.end(),
      purchaseValueSquaredArrays.begin());

  if (groupType == GroupType::TEST) {
    metrics_.testValueSquared = sum(squaredValues);
  } else {
    metrics_.controlValueSquared = sum(squaredValues);
  }

  // And compute for breakdowns + cohorts
  // TODO: These could be abstracted into a common function
  for (size_t i = 0; i < numPublisherBreakdowns_; ++i) {
    const auto& mask = publisherBitmasks_.at(i);
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        squaredValues, mask);
    if (groupType == GroupType::TEST) {
      publisherBreakdowns_[i].testValueSquared = sum(groupInts);
    } else {
      publisherBreakdowns_[i].controlValueSquared = sum(groupInts);
    }
  }
  for (size_t i = 0; i < numPartnerCohorts_; ++i) {
    const auto& mask = partnerBitmasks_.at(i);
    auto groupInts = private_measurement::secret_sharing::multiplyBitmask(
        squaredValues, mask);
    if (groupType == GroupType::TEST) {
      cohortMetrics_[i].testValueSquared = sum(groupInts);
    } else {
      cohortMetrics_[i].controlValueSquared = sum(groupInts);
    }
  }
}

template <int32_t MY_ROLE>
int64_t OutputMetrics<MY_ROLE>::sum(const std::vector<emp::Integer>& in) const {
  return shouldUseXorEncryption()
      ? private_measurement::emp_utils::sum<emp::XOR>(in)
      : private_measurement::emp_utils::sum<emp::PUBLIC>(in);
}

template <int32_t MY_ROLE>
int64_t OutputMetrics<MY_ROLE>::sum(const std::vector<emp::Bit>& in) const {
  return sum(private_measurement::emp_utils::bitsToInts(in));
}

template <int32_t MY_ROLE>
int64_t OutputMetrics<MY_ROLE>::sum(
    const std::vector<std::vector<emp::Bit>>& in) const {
  // flatten the 2D vector into 1D
  // TODO: this can be optimizing by specializing this use case so we don't have
  // to make a copy of the data
  std::vector<emp::Bit> accum;
  for (auto& sub : in) {
    accum.insert(std::end(accum), std::begin(sub), std::end(sub));
  }
  return sum(accum);
}

template <int32_t MY_ROLE>
int64_t OutputMetrics<MY_ROLE>::sum(
    const std::vector<std::vector<emp::Integer>>& in) const {
  // flatten the 2D vector into 1D
  // TODO: this can be optimizing by specializing this use case so we don't have
  // to make a copy of the data
  std::vector<emp::Integer> accum;
  for (auto& sub : in) {
    accum.insert(std::end(accum), std::begin(sub), std::end(sub));
  }
  return sum(accum);
}

} // namespace private_lift
