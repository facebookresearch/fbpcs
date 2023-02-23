/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "folly/logging/xlog.h"

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"

namespace private_lift::input_processing {

inline std::vector<unsigned char> convertFromVectorOfBits(
    std::vector<bool> data) {
  std::vector<unsigned char> rst;
  rst.reserve(data.size() / 8);

  size_t i = 0;

  while (i < data.size()) {
    unsigned char val = 0;
    size_t bitsLeft = data.size() - i > 8 ? 8 : data.size() - i;
    for (auto j = 0; j < bitsLeft; j++) {
      val |= (data[i] << j);
      ++i;
    }
    rst.push_back(val);
  }

  return rst;
}

template <int schedulerId>
using SecString = typename fbpcf::mpc_std_lib::unified_data_process::
    data_processor::IDataProcessor<schedulerId>::SecString;

struct PartnerRow {
  bool anyValidPurchaseTimestamp;
  uint32_t cohortGroupId;
};

struct PartnerConversionRow {
  uint32_t purchaseTimestamp;
  uint32_t thresholdTimestamp;
  int32_t purchaseValue;
  int64_t purchaseValueSquared;
};

struct PublisherRow {
  bool breakdownId;
  bool controlPopulation;
  bool isValidOpportunityTimestamp;
  bool testReach;
  uint32_t opportunityTimestamp;
};

const int PARTNER_ROW_SIZE_BYTES = 5;
const int PARTNER_CONVERSION_ROW_SIZE_BYTES = 20;
const int PUBLISHER_ROW_BYTES = 5;

template <typename T>
unsigned char extractByte(T val, size_t byte) {
  if (byte < 0 || byte >= sizeof(T)) {
    throw std::invalid_argument("Not enough bytes in type");
  }

  return (uint8_t)(val >> 8 * byte);
}

template <typename T>
T reconstructFromBytes(unsigned char* data) {
  T val = 0;
  for (size_t i = 0; i < sizeof(T); i++) {
    val |= ((T) * (data + i)) << (i * 8);
  }
  return val;
}

template <int schedulerId>
std::tuple<
    std::vector<PartnerRow>,
    std::vector<std::vector<PartnerConversionRow>>,
    std::vector<PublisherRow>>
deserializeSecretSharedData(
    LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    const SecString<schedulerId>& publisherDataShares,
    const SecString<schedulerId>& partnerDataShares,
    int32_t numConversionsPerUser) {
  std::vector<std::vector<bool>> publisherSecretSharedBits =
      publisherDataShares.extractStringShare().getValue();
  publisherSecretSharedBits = common::transpose(publisherSecretSharedBits);

  std::vector<std::vector<bool>> partnerSecretSharedBits =
      partnerDataShares.extractStringShare().getValue();
  partnerSecretSharedBits = common::transpose(partnerSecretSharedBits);

  std::vector<std::vector<PartnerConversionRow>> partnerConversionRows(
      liftGameProcessedData.numRows);
  std::vector<PartnerRow> partnerRows(liftGameProcessedData.numRows);
  std::vector<PublisherRow> publisherRows(liftGameProcessedData.numRows);

  for (size_t i = 0; i < liftGameProcessedData.numRows; i++) {
    auto publisherByteShares =
        convertFromVectorOfBits(publisherSecretSharedBits.at(i));
    auto partnerByteShares =
        convertFromVectorOfBits(partnerSecretSharedBits.at(i));

    publisherRows[i].breakdownId = publisherByteShares[0] & 1;
    publisherRows[i].controlPopulation = (publisherByteShares[0] >> 1) & 1;
    publisherRows[i].isValidOpportunityTimestamp =
        (publisherByteShares[0] >> 2) & 1;
    publisherRows[i].testReach = (publisherByteShares[0] >> 3) & 1;
    publisherRows[i].opportunityTimestamp =
        reconstructFromBytes<uint32_t>(publisherByteShares.data() + 1);

    partnerRows[i].anyValidPurchaseTimestamp = partnerByteShares[0] & 1;
    partnerRows[i].cohortGroupId =
        reconstructFromBytes<uint32_t>(partnerByteShares.data() + 1);

    partnerConversionRows[i] =
        std::vector<PartnerConversionRow>(numConversionsPerUser);

    for (size_t j = 0; j < numConversionsPerUser; j++) {
      partnerConversionRows[i][j].purchaseTimestamp =
          reconstructFromBytes<uint32_t>(
              partnerByteShares.data() + 5 +
              j * PARTNER_CONVERSION_ROW_SIZE_BYTES);
      partnerConversionRows[i][j].thresholdTimestamp =
          reconstructFromBytes<uint32_t>(
              partnerByteShares.data() + 9 +
              j * PARTNER_CONVERSION_ROW_SIZE_BYTES);

      partnerConversionRows[i][j].purchaseValue = reconstructFromBytes<int32_t>(
          partnerByteShares.data() + 13 +
          j * PARTNER_CONVERSION_ROW_SIZE_BYTES);

      partnerConversionRows[i][j].purchaseValueSquared =
          reconstructFromBytes<int64_t>(
              partnerByteShares.data() + 17 +
              j * PARTNER_CONVERSION_ROW_SIZE_BYTES);
    }
  }

  return std::make_tuple<
      std::vector<PartnerRow>,
      std::vector<std::vector<PartnerConversionRow>>,
      std::vector<PublisherRow>>(
      std::move(partnerRows),
      std::move(partnerConversionRows),
      std::move(publisherRows));
}

template <int schedulerId>
void extractPartnerValues(
    LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    SecGroup<schedulerId>& cohortGroupIds,
    const std::vector<PartnerRow>& partnerRows) {
  std::vector<bool> anyValidPurchaseTimestampShares(
      liftGameProcessedData.numRows);
  std::vector<uint64_t> groupIdShares(liftGameProcessedData.numRows);

  for (int row = 0; row < liftGameProcessedData.numRows; row++) {
    anyValidPurchaseTimestampShares[row] =
        partnerRows[row].anyValidPurchaseTimestamp;
    groupIdShares[row] = partnerRows[row].cohortGroupId;
  }

  liftGameProcessedData.anyValidPurchaseTimestamp =
      SecBit<schedulerId>(typename SecBit<schedulerId>::ExtractedBit(
          anyValidPurchaseTimestampShares));

  cohortGroupIds = SecGroup<schedulerId>(
      typename SecGroup<schedulerId>::ExtractedInt(groupIdShares));
}

template <int schedulerId>
void extractPartnerConversionValues(
    LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    const std::vector<std::vector<PartnerConversionRow>>& partnerConversionRows,
    int32_t numConversionsPerUser) {
  liftGameProcessedData.purchaseTimestamps =
      std::vector<SecTimestamp<schedulerId>>(numConversionsPerUser);
  liftGameProcessedData.thresholdTimestamps =
      std::vector<SecTimestamp<schedulerId>>(numConversionsPerUser);
  liftGameProcessedData.purchaseValues =
      std::vector<SecValue<schedulerId>>(numConversionsPerUser);
  liftGameProcessedData.purchaseValueSquared =
      std::vector<SecValueSquared<schedulerId>>(numConversionsPerUser);

  for (int conversion = 0; conversion < numConversionsPerUser; conversion++) {
    std::vector<uint64_t> purchaseTimestampShares(
        liftGameProcessedData.numRows);
    std::vector<uint64_t> thresholdTimestampShares(
        liftGameProcessedData.numRows);
    std::vector<int64_t> purchaseValueShares(liftGameProcessedData.numRows);
    std::vector<int64_t> purchaseValueSquaredShares(
        liftGameProcessedData.numRows);

    for (int row = 0; row < liftGameProcessedData.numRows; row++) {
      purchaseTimestampShares[row] =
          partnerConversionRows[row][conversion].purchaseTimestamp;
      thresholdTimestampShares[row] =
          partnerConversionRows[row][conversion].thresholdTimestamp;
      purchaseValueShares[row] =
          partnerConversionRows[row][conversion].purchaseValue;
      purchaseValueSquaredShares[row] =
          partnerConversionRows[row][conversion].purchaseValueSquared;
    }

    liftGameProcessedData.purchaseTimestamps[conversion] =
        SecTimestamp<schedulerId>(
            typename SecTimestamp<schedulerId>::ExtractedInt(
                purchaseTimestampShares));
    liftGameProcessedData.thresholdTimestamps[conversion] =
        SecTimestamp<schedulerId>(
            typename SecTimestamp<schedulerId>::ExtractedInt(
                thresholdTimestampShares));
    liftGameProcessedData.purchaseValues[conversion] = SecValue<schedulerId>(
        typename SecValue<schedulerId>::ExtractedInt(purchaseValueShares));

    liftGameProcessedData.purchaseValueSquared[conversion] =
        SecValueSquared<schedulerId>(
            typename SecValueSquared<schedulerId>::ExtractedInt(
                purchaseValueSquaredShares));
  }
}

template <int schedulerId>
void extractPublisherValues(
    LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    SecBit<schedulerId>& controlPopulation,
    SecBit<schedulerId>& breakdownGroupIds,
    const std::vector<PublisherRow>& publisherRows) {
  std::vector<bool> breakdownGroupIdShares(liftGameProcessedData.numRows);
  std::vector<bool> controlPopulationShares(liftGameProcessedData.numRows);
  std::vector<bool> isValidOpportunityTimestampShares(
      liftGameProcessedData.numRows);
  std::vector<bool> testReachShares(liftGameProcessedData.numRows);
  std::vector<uint64_t> opportunityTimestampShares(
      liftGameProcessedData.numRows);

  for (int row = 0; row < liftGameProcessedData.numRows; row++) {
    breakdownGroupIdShares[row] = publisherRows[row].breakdownId;
    controlPopulationShares[row] = publisherRows[row].controlPopulation;
    isValidOpportunityTimestampShares[row] =
        publisherRows[row].isValidOpportunityTimestamp;
    testReachShares[row] = publisherRows[row].testReach;
    opportunityTimestampShares[row] = publisherRows[row].opportunityTimestamp;
  }

  breakdownGroupIds = SecBit<schedulerId>(
      typename SecBit<schedulerId>::ExtractedBit(breakdownGroupIdShares));
  controlPopulation = SecBit<schedulerId>(
      typename SecBit<schedulerId>::ExtractedBit(controlPopulationShares));
  liftGameProcessedData.isValidOpportunityTimestamp =
      SecBit<schedulerId>(typename SecBit<schedulerId>::ExtractedBit(
          isValidOpportunityTimestampShares));
  liftGameProcessedData.testReach = SecBit<schedulerId>(
      typename SecBit<schedulerId>::ExtractedBit(testReachShares));
  liftGameProcessedData.opportunityTimestamps = SecTimestamp<schedulerId>(
      typename SecTimestamp<schedulerId>::ExtractedInt(
          opportunityTimestampShares));
}

template <int schedulerId>
void extractCompactedData(
    LiftGameProcessedData<schedulerId>& liftGameProcessedData,
    SecBit<schedulerId>& controlPopulation,
    SecGroup<schedulerId>& cohortGroupIds,
    SecBit<schedulerId>& breakdownGroupIds,
    const SecString<schedulerId>& publisherDataShares,
    const SecString<schedulerId>& partnerDataShares,
    int32_t numConversionsPerUser) {
  liftGameProcessedData.numRows = publisherDataShares.getBatchSize();

  std::tuple<
      std::vector<PartnerRow>,
      std::vector<std::vector<PartnerConversionRow>>,
      std::vector<PublisherRow>>
      deserializedSecretStructs = deserializeSecretSharedData(
          liftGameProcessedData,
          publisherDataShares,
          partnerDataShares,
          numConversionsPerUser);

  extractPartnerValues(
      liftGameProcessedData,
      cohortGroupIds,
      std::get<0>(deserializedSecretStructs));
  extractPartnerConversionValues(
      liftGameProcessedData,
      std::get<1>(deserializedSecretStructs),
      numConversionsPerUser);
  extractPublisherValues(
      liftGameProcessedData,
      controlPopulation,
      breakdownGroupIds,
      std::get<2>(deserializedSecretStructs));
}

} // namespace private_lift::input_processing
