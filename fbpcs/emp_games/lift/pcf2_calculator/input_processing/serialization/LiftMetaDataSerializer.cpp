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

using input_processing::extractByte;
using input_processing::PARTNER_CONVERSION_ROW_SIZE_BYTES;
using input_processing::PARTNER_ROW_SIZE_BYTES;
using input_processing::PartnerRow;
using input_processing::PUBLISHER_ROW_BYTES;
using input_processing::PublisherRow;

std::vector<std::vector<unsigned char>>
LiftMetaDataSerializer::serializePublisherMetadata() {
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
  for (size_t i = 0; i < inputSize; i++) {
    int inputIndex =
        reverseUnionMap_ == std::nullopt ? i : reverseUnionMap_->at(i);

    bool isValidOpportunityTimestamp =
        (opportunityTimestampsPadded.at(inputIndex) > 0) &&
        (controlPopulationPadded.at(inputIndex) ||
         testPopulationPadded.at(inputIndex));

    bool testReach = testPopulationPadded.at(inputIndex) &&
        (numImpressionsPadded.at(inputIndex) > 0);

    PublisherRow publisherRow{
        .breakdownId = (bool)breakdownIdPadded.at(inputIndex),
        .controlPopulation = controlPopulationPadded.at(inputIndex),
        .isValidOpportunityTimestamp = isValidOpportunityTimestamp,
        .testReach = testReach,
        .opportunityTimestamp = opportunityTimestampsPadded.at(inputIndex),
    };

    std::vector<unsigned char> serialized(PUBLISHER_ROW_BYTES);
    serialized[0] = publisherRow.breakdownId |
        (publisherRow.controlPopulation << 1) |
        (publisherRow.isValidOpportunityTimestamp << 2) |
        (publisherRow.testReach << 3);

    for (size_t byte = 0; byte < 4; byte++) {
      serialized[1 + byte] =
          extractByte(publisherRow.opportunityTimestamp, byte);
    }

    rst.push_back(serialized);
  }
  return rst;
}

std::vector<std::vector<unsigned char>>
LiftMetaDataSerializer::serializePartnerMetadata() {
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

  for (size_t i = 0; i < inputSize; i++) {
    int inputIndex =
        reverseUnionMap_ == std::nullopt ? i : reverseUnionMap_->at(i);

    bool anyValidPurchaseTimestamp = false;
    for (uint32_t purchaseTs : purchaseTimestampsPadded.at(inputIndex)) {
      // compute whether each row contains at least one valid (positive)
      // purchase timestamp
      anyValidPurchaseTimestamp |= (purchaseTs > 0);
    }

    PartnerRow partnerRow{
        .anyValidPurchaseTimestamp = anyValidPurchaseTimestamp,
        .cohortGroupId = cohortIdsPadded.at(inputIndex)};

    std::vector<input_processing::PartnerConversionRow> rowConversions(
        numConversionsPerUser_);

    for (size_t j = 0; j < numConversionsPerUser_; j++) {
      rowConversions[j] = {
          .purchaseTimestamp = purchaseTimestampsPadded.at(inputIndex).at(j),
          .thresholdTimestamp =
              purchaseTimestampsPadded.at(inputIndex).at(j) > 0
              ? purchaseTimestampsPadded.at(inputIndex).at(j) +
                  kPurchaseTimestampThresholdWindow
              : 0,
          .purchaseValue = (int32_t)purchaseValuesPadded.at(inputIndex).at(j),
          .purchaseValueSquared =
              purchaseValuesSquaredPadded.at(inputIndex).at(j)};
    }

    std::vector<unsigned char> serialized(
        PARTNER_ROW_SIZE_BYTES +
        PARTNER_CONVERSION_ROW_SIZE_BYTES * numConversionsPerUser_);

    serialized[0] = partnerRow.anyValidPurchaseTimestamp;
    serialized[1] = extractByte(partnerRow.cohortGroupId, 0);
    serialized[2] = extractByte(partnerRow.cohortGroupId, 1);
    serialized[3] = extractByte(partnerRow.cohortGroupId, 2);
    serialized[4] = extractByte(partnerRow.cohortGroupId, 3);

    for (size_t j = 0; j < numConversionsPerUser_; j++) {
      for (int byte = 0; byte < 4; byte++) {
        serialized[5 + j * PARTNER_CONVERSION_ROW_SIZE_BYTES + byte] =
            extractByte(rowConversions[j].purchaseTimestamp, byte);
        serialized[9 + j * PARTNER_CONVERSION_ROW_SIZE_BYTES + byte] =
            extractByte(rowConversions[j].thresholdTimestamp, byte);
        serialized[13 + j * PARTNER_CONVERSION_ROW_SIZE_BYTES + byte] =
            extractByte(rowConversions[j].purchaseValue, byte);
      }

      for (size_t byte = 0; byte < 8; byte++) {
        serialized[17 + j * PARTNER_CONVERSION_ROW_SIZE_BYTES + byte] =
            extractByte(rowConversions[j].purchaseValueSquared, byte);
      }
    }

    rst.push_back(serialized);
  }
  return rst;
}

} // namespace private_lift
