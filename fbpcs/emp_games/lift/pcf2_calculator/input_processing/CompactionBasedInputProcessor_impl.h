/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <numeric>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "fbpcf/mpc_std_lib/util/secureRandomPermutation.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor.h"

namespace private_lift {

template <int schedulerId>
std::vector<int32_t>
CompactionBasedInputProcessor<schedulerId>::shuffleAndGetUnionMap() {
  XLOG(INFO) << "Shuffling input and preparing Union Map for Adapter input";
  int32_t unionSize = inputData_.getNumRows();
  const std::vector<uint32_t> randomPermutation =
      fbpcf::mpc_std_lib::util::secureRandomPermutation(unionSize, *prg_);
  std::vector<int32_t> unionMap(unionSize);
  const std::vector<bool>& dummyRows = inputData_.getDummyRows();
  int32_t nonDummyRows = 0;
  for (int32_t i = 0; i < unionMap.size(); i++) {
    unionMap[randomPermutation[i]] =
        dummyRows[randomPermutation[i]] ? -1 : nonDummyRows++;
  }
  return unionMap;
}

template <int schedulerId>
std::vector<int32_t>
CompactionBasedInputProcessor<schedulerId>::getIntersectionMap(
    const std::vector<int32_t>& unionMap) {
  XLOG(INFO) << "Begin adapter protocol";
  return adapter_->adapt(unionMap);
}

template <int schedulerId>
std::vector<std::vector<unsigned char>>
CompactionBasedInputProcessor<schedulerId>::preparePlaintextData(
    const std::vector<int32_t>& unionMap) {
  XLOG(INFO) << "Begin plaintext data serialization as bytes";
  size_t unionSize = inputData_.getNumRows();
  int32_t inputSize = 0;
  std::vector<int32_t> reverseUnionMap(inputData_.getNumRows());

  for (int i = 0; i < unionMap.size(); i++) {
    if (unionMap[i] >= 0) {
      reverseUnionMap[unionMap[i]] = i;
      inputSize = std::max(inputSize, unionMap[i]);
    }
  }

  inputSize++;
  reverseUnionMap.resize(inputSize);

  using InputColumnDataType =
      typename fbpcf::mpc_std_lib::unified_data_process::serialization::
          IColumnDefinition<schedulerId>::InputColumnDataType;

  if (myRole_ == common::PARTNER) {
    auto cohortIdsPadded =
        common::padArray<uint32_t>(inputData_.getGroupIds(), unionSize, 0);
    auto purchaseTimestampsPadded = common::padNestedArrays<uint32_t>(
        inputData_.getPurchaseTimestampArrays(),
        unionSize,
        numConversionsPerUser_,
        0);
    auto purchaseValuesPadded = common::padNestedArrays<int64_t>(
        inputData_.getPurchaseValueArrays(),
        unionSize,
        numConversionsPerUser_,
        0);
    auto purchaseValuesSquaredPadded = common::padNestedArrays<int64_t>(
        inputData_.getPurchaseValueSquaredArrays(),
        unionSize,
        numConversionsPerUser_,
        0);

    std::vector<bool> anyValidPurchaseTimestamps(inputSize);
    std::vector<uint32_t> cohortIdsSorted(inputSize);
    std::vector<std::vector<uint32_t>> purchaseTimestampsSorted(
        inputSize, std::vector<uint32_t>(numConversionsPerUser_));
    std::vector<std::vector<uint32_t>> thresholdTimestampsSorted(
        inputSize, std::vector<uint32_t>(numConversionsPerUser_));
    std::vector<std::vector<int32_t>> purchaseValuesSorted(
        inputSize, std::vector<int32_t>(numConversionsPerUser_));
    std::vector<std::vector<int64_t>> purchaseValuesSquaredSorted(
        inputSize, std::vector<int64_t>(numConversionsPerUser_));

    for (size_t i = 0; i < inputSize; i++) {
      int inputIndex = reverseUnionMap[i];

      cohortIdsSorted[i] = cohortIdsPadded[inputIndex];

      bool anyValidPurchaseTimestamp = false;
      for (int j = 0; j < numConversionsPerUser_; j++) {
        // compute whether each row contains at least one valid (positive)
        // purchase timestamp
        anyValidPurchaseTimestamp |=
            (purchaseTimestampsPadded[inputIndex][j] > 0);

        purchaseTimestampsSorted[i][j] =
            purchaseTimestampsPadded[inputIndex][j];

        thresholdTimestampsSorted[i][j] =
            purchaseTimestampsPadded[inputIndex][j] > 0
            ? purchaseTimestampsPadded[inputIndex][j] +
                kPurchaseTimestampThresholdWindow
            : 0;
        purchaseValuesSorted[i][j] = purchaseValuesPadded[inputIndex][j];
        purchaseValuesSquaredSorted[i][j] =
            purchaseValuesSquaredPadded[inputIndex][j];
      }
      anyValidPurchaseTimestamps[i] = anyValidPurchaseTimestamp;
    }

    std::unordered_map<std::string, InputColumnDataType> inputMap{
        {"anyValidPurchaseTimestamp", anyValidPurchaseTimestamps},
        {"cohortGroupId", cohortIdsSorted},
        {"purchaseTimestamp", purchaseTimestampsPadded},
        {"thresholdTimestamp", thresholdTimestampsSorted},
        {"purchaseValue", purchaseValuesSorted},
        {"purchaseValueSquared", purchaseValuesSquaredSorted},
    };

    return partnerSerializer_->serializeDataAsBytesForUDP(inputMap, inputSize);
  } else {
    auto opportunityTimestampsPadded = common::padArray<uint32_t>(
        inputData_.getOpportunityTimestamps(), unionSize, 0);
    auto controlPopulationPadded = common::padArray<bool>(
        inputData_.getControlPopulation(), unionSize, false);
    auto testPopulationPadded = common::padArray<bool>(
        inputData_.getTestPopulation(), unionSize, false);
    auto numImpressionsPadded =
        common::padArray<int64_t>(inputData_.getNumImpressions(), unionSize, 0);
    auto breakdownIdPadded =
        common::padArray<uint32_t>(inputData_.getBreakdownIds(), unionSize, 0);

    std::vector<bool> breakdownIdSorted(inputSize);
    std::vector<bool> controlPopulationSorted(inputSize);
    std::vector<bool> isValidOpportunityTimestamp(inputSize);
    std::vector<bool> testReach(inputSize);
    std::vector<uint32_t> opportunityTimestampsSorted(inputSize);
    for (size_t i = 0; i < inputSize; i++) {
      int inputIndex = reverseUnionMap[i];

      breakdownIdSorted[i] = breakdownIdPadded[inputIndex];
      controlPopulationSorted[i] = controlPopulationPadded[inputIndex];
      isValidOpportunityTimestamp[i] =
          (opportunityTimestampsPadded.at(inputIndex) > 0) &&
          (controlPopulationPadded.at(inputIndex) ||
           testPopulationPadded.at(inputIndex));

      testReach[i] = testPopulationPadded.at(inputIndex) &&
          (numImpressionsPadded.at(inputIndex) > 0);
      opportunityTimestampsSorted[i] = opportunityTimestampsPadded[inputIndex];
    }

    std::unordered_map<std::string, InputColumnDataType> inputMap{
        {"breakdownId", breakdownIdSorted},
        {"controlPopulation", controlPopulationSorted},
        {"isValidOpportunityTimestamp", isValidOpportunityTimestamp},
        {"testReach", testReach},
        {"opportunityTimestamp", opportunityTimestampsSorted},
    };

    return publisherSerializer_->serializeDataAsBytesForUDP(
        inputMap, inputSize);
  }
}

template <int schedulerId>
std::pair<
    typename CompactionBasedInputProcessor<schedulerId>::SecString,
    typename CompactionBasedInputProcessor<schedulerId>::SecString>
CompactionBasedInputProcessor<schedulerId>::compactData(
    const std::vector<int32_t>& intersectionMap,
    const std::vector<std::vector<unsigned char>>& plaintextData) {
  XLOG(INFO) << "Beginning oblivious data intersection step";

  int32_t myRows = plaintextData.size();

  auto publisherRows = common::shareIntFrom<
      schedulerId,
      sizeof(myRows) * 8,
      common::PUBLISHER,
      common::PARTNER>(myRole_, myRows);

  auto partnerRows = common::shareIntFrom<
      schedulerId,
      sizeof(myRows) * 8,
      common::PARTNER,
      common::PUBLISHER>(myRole_, myRows);

  XLOG(INFO) << "Publisher Row count: " << publisherRows;
  XLOG(INFO) << "Publisher Row size in bytes: "
             << publisherSerializer_->getRowSizeBytes();

  XLOG(INFO) << "Partner Row count: " << partnerRows;
  XLOG(INFO) << "Partner Row size in bytes: "
             << partnerSerializer_->getRowSizeBytes();

  SecString publisherDataShares;
  SecString partnerDataShares;

  if (myRole_ == common::PUBLISHER) {
    XLOG(INFO) << "Begin processing my data (publisher)";
    publisherDataShares =
        dataProcessor_->processMyData(plaintextData, intersectionMap.size());
    XLOG(INFO) << "Begin processing peers data (partner)";
    partnerDataShares = dataProcessor_->processPeersData(
        partnerRows, intersectionMap, partnerSerializer_->getRowSizeBytes());
  } else if (myRole_ == common::PARTNER) {
    XLOG(INFO) << "Begin processing peers data (publisher)";
    publisherDataShares = dataProcessor_->processPeersData(
        publisherRows,
        intersectionMap,
        publisherSerializer_->getRowSizeBytes());
    XLOG(INFO) << "Begin processing my data (partner)";
    partnerDataShares =
        dataProcessor_->processMyData(plaintextData, intersectionMap.size());
  }

  auto expectedIntersectionSize = std::transform_reduce(
      intersectionMap.begin(),
      intersectionMap.end(),
      0,
      [](const int32_t& left, const int32_t& right) { return left + right; },
      [](const int32_t& ele) { return ele == -1 ? 0 : 1; });

  if (expectedIntersectionSize != publisherDataShares.getBatchSize()) {
    throw std::runtime_error(folly::sformat(
        "Publisher rows do not match up expected intersection size. Expected {} but got {} rows.",
        expectedIntersectionSize,
        publisherDataShares.getBatchSize()));
  }

  if (expectedIntersectionSize != partnerDataShares.getBatchSize()) {
    throw std::runtime_error(folly::sformat(
        "Partner rows do not match up expected intersection size. Expected {} but got {} rows.",
        expectedIntersectionSize,
        partnerDataShares.getBatchSize()));
  }

  XLOG(INFO) << folly::format(
      "{} rows in intersection after running data processor",
      expectedIntersectionSize);

  return std::make_pair<
      typename CompactionBasedInputProcessor<schedulerId>::SecString,
      typename CompactionBasedInputProcessor<schedulerId>::SecString>(
      std::move(publisherDataShares), std::move(partnerDataShares));
}

template <int schedulerId>
void CompactionBasedInputProcessor<schedulerId>::extractCompactedData(
    const typename CompactionBasedInputProcessor<schedulerId>::SecString&
        publisherDataShares,
    const typename CompactionBasedInputProcessor<schedulerId>::SecString&
        partnerDataShares) {
  XLOG(INFO, "Begin extraction to MPC types");

  liftGameProcessedData_.numRows = publisherDataShares.getBatchSize();

  auto publisherDeserialized =
      publisherSerializer_->deserializeUDPOutputIntoMPCTypes(
          publisherDataShares);
  auto partnerDeserialized =
      partnerSerializer_->deserializeUDPOutputIntoMPCTypes(partnerDataShares);

  using MPCTypes = fbpcf::frontend::MPCTypes<schedulerId, true>;

  breakdownGroupIds_ = std::get<typename MPCTypes::SecBool>(
      publisherDeserialized.at("breakdownId"));
  controlPopulation_ = std::get<typename MPCTypes::SecBool>(
      publisherDeserialized.at("controlPopulation"));
  cohortGroupIds_ = std::get<typename MPCTypes::SecUnsigned32Int>(
      partnerDeserialized.at("cohortGroupId"));

  liftGameProcessedData_.isValidOpportunityTimestamp =
      std::get<typename MPCTypes::SecBool>(
          publisherDeserialized.at("isValidOpportunityTimestamp"));
  liftGameProcessedData_.testReach = std::get<typename MPCTypes::SecBool>(
      publisherDeserialized.at("testReach"));
  liftGameProcessedData_.opportunityTimestamps =
      std::get<typename MPCTypes::SecUnsigned32Int>(
          publisherDeserialized.at("opportunityTimestamp"));

  liftGameProcessedData_.anyValidPurchaseTimestamp =
      std::get<typename MPCTypes::SecBool>(
          partnerDeserialized.at("anyValidPurchaseTimestamp"));
  liftGameProcessedData_.purchaseTimestamps =
      std::get<std::vector<typename MPCTypes::SecUnsigned32Int>>(
          partnerDeserialized.at("purchaseTimestamp"));
  liftGameProcessedData_.thresholdTimestamps =
      std::get<std::vector<typename MPCTypes::SecUnsigned32Int>>(
          partnerDeserialized.at("thresholdTimestamp"));
  liftGameProcessedData_.purchaseValues =
      std::get<std::vector<typename MPCTypes::Sec32Int>>(
          partnerDeserialized.at("purchaseValue"));
  liftGameProcessedData_.purchaseValueSquared =
      std::get<std::vector<typename MPCTypes::Sec64Int>>(
          partnerDeserialized.at("purchaseValueSquared"));

  XLOG(INFO, "Finish extraction to MPC types");
}
} // namespace private_lift
