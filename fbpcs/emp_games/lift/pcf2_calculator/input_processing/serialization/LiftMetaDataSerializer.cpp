/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/serialization/LiftMetaDataSerializer.h"
#include <vector>
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/LiftCompactionUtils.h"

namespace private_lift {

std::vector<std::vector<unsigned char>>
LiftMetaDataSerializer::serializePublisherMetadata() {
  // hardcode the schedulerId as no MPC types are created during serialization
  auto publisherSerializer =
      input_processing::createPublisherSerializer<0>(numConversionsPerUser_);

  std::vector<std::vector<unsigned char>> rst;
  size_t inputSize = reverseUnionMap_ == std::nullopt
      ? inputData_.getNumRows()
      : reverseUnionMap_->size();
  size_t unionSize =
      unionSize_ == std::nullopt ? inputSize : unionSize_.value();

  rst.reserve(inputSize);

  auto opportunityTimestampsPadded = common::padArray<uint32_t>(
      inputData_.getOpportunityTimestamps(), unionSize, 0);
  auto controlPopulationPadded = common::padArray<bool>(
      inputData_.getControlPopulation(), unionSize, false);
  auto testPopulationPadded =
      common::padArray<bool>(inputData_.getTestPopulation(), unionSize, false);
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
    int inputIndex =
        reverseUnionMap_ == std::nullopt ? i : reverseUnionMap_->at(i);

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

  using InputColumnDataType =
      typename fbpcf::mpc_std_lib::unified_data_process::serialization::
          IColumnDefinition<0>::InputColumnDataType;

  std::unordered_map<std::string, InputColumnDataType> inputMap{
      {"breakdownId", breakdownIdSorted},
      {"controlPopulation", controlPopulationSorted},
      {"isValidOpportunityTimestamp", isValidOpportunityTimestamp},
      {"testReach", testReach},
      {"opportunityTimestamp", opportunityTimestampsSorted},
  };

  return publisherSerializer->serializeDataAsBytesForUDP(inputMap, inputSize);
}

std::vector<std::vector<unsigned char>>
LiftMetaDataSerializer::serializePartnerMetadata() {
  // hardcode the schedulerId as no MPC types are created during serialization
  auto partnerSerializer =
      input_processing::createPartnerSerializer<0>(numConversionsPerUser_);

  std::vector<std::vector<unsigned char>> rst;
  size_t inputSize = reverseUnionMap_ == std::nullopt
      ? inputData_.getNumRows()
      : reverseUnionMap_->size();
  size_t unionSize =
      unionSize_ == std::nullopt ? inputSize : unionSize_.value();
  rst.reserve(inputSize);

  auto cohortIdsPadded = common::padArray<uint32_t>(
      inputData_.getPartnerCohortIds(), unionSize, 0);
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
    int inputIndex =
        reverseUnionMap_ == std::nullopt ? i : reverseUnionMap_->at(i);

    cohortIdsSorted[i] = cohortIdsPadded[inputIndex];

    bool anyValidPurchaseTimestamp = false;
    for (int j = 0; j < numConversionsPerUser_; j++) {
      // compute whether each row contains at least one valid (positive)
      // purchase timestamp
      anyValidPurchaseTimestamp |=
          (purchaseTimestampsPadded[inputIndex][j] > 0);

      purchaseTimestampsSorted[i][j] = purchaseTimestampsPadded[inputIndex][j];

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

  using InputColumnDataType =
      typename fbpcf::mpc_std_lib::unified_data_process::serialization::
          IColumnDefinition<0>::InputColumnDataType;

  std::unordered_map<std::string, InputColumnDataType> inputMap{
      {"anyValidPurchaseTimestamp", anyValidPurchaseTimestamps},
      {"cohortGroupId", cohortIdsSorted},
      {"purchaseTimestamp", purchaseTimestampsSorted},
      {"thresholdTimestamp", thresholdTimestampsSorted},
      {"purchaseValue", purchaseValuesSorted},
      {"purchaseValueSquared", purchaseValuesSquaredSorted},
  };

  return partnerSerializer->serializeDataAsBytesForUDP(inputMap, inputSize);
}

} // namespace private_lift
