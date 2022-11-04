/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
#include "fbpcf/engine/util/IPrg.h"
#include "folly/logging/xlog.h"

#include "fbpcf/mpc_std_lib/unified_data_process/adapter/IAdapter.h"
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/IDataProcessor.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/GlobalSharingUtils.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"

namespace private_lift {
/**
 * This class handles privately sharing all the input data in MPC. It will
 * handle obliviously filtering out rows with dummy entries.
 */
template <int schedulerId>
class CompactionBasedInputProcessor : public IInputProcessor<schedulerId> {
 public:
  using SecString = typename fbpcf::mpc_std_lib::unified_data_process::
      data_processor::IDataProcessor<schedulerId>::SecString;

  CompactionBasedInputProcessor(
      int myRole,
      std::unique_ptr<
          fbpcf::mpc_std_lib::unified_data_process::adapter::IAdapter> adapter,
      std::unique_ptr<fbpcf::mpc_std_lib::unified_data_process::data_processor::
                          IDataProcessor<schedulerId>> dataProcessor,
      std::unique_ptr<fbpcf::engine::util::IPrg> prg,
      InputData inputData,
      int32_t numConversionsPerUser)
      : myRole_{myRole},
        adapter_{std::move(adapter)},
        dataProcessor_{std::move(dataProcessor)},
        prg_{std::move(prg)},
        inputData_{inputData},
        numConversionsPerUser_{numConversionsPerUser} {
    if (inputData.getNumRows() == 0) {
      liftGameProcessedData_ = {};
      return;
    }

    liftGameProcessedData_.numRows = inputData.getNumRows();

    input_processing::validateNumRowsStep(myRole_, liftGameProcessedData_);
    input_processing::shareNumGroupsStep(
        myRole_, inputData_, liftGameProcessedData_);
    input_processing::shareBitsForValuesStep(
        myRole_, inputData_, liftGameProcessedData_);

    auto unionMap = shuffleAndGetUnionMap();

    auto intersectionMap = getIntersectionMap(unionMap);

    if (intersectionMap.size() == 0) {
      liftGameProcessedData_ = {};
      return;
    }

    auto plaintextData = preparePlaintextData(unionMap);

    auto publisherPartnerJointMetadataShares =
        compactData(intersectionMap, plaintextData);

    extractCompactedData(
        std::get<0>(publisherPartnerJointMetadataShares),
        std::get<1>(publisherPartnerJointMetadataShares));

    input_processing::computeIndexSharesAndSetTestGroupIds(
        liftGameProcessedData_,
        cohortGroupIds_,
        controlPopulation_,
        breakdownGroupIds_,
        testGroupIds_);
    input_processing::computeTestIndexShares(
        liftGameProcessedData_, controlPopulation_, testGroupIds_);
  }

  // CompactionBasedInputProcessor();

  const LiftGameProcessedData<schedulerId>& getLiftGameProcessedData()
      const override {
    return liftGameProcessedData_;
  }

 private:
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

  // Update the values if changing the structs above. This class handles it's
  // own serialization / deserialization. using sizeof() will not work because a
  // bool will take 1 byte in memory
  const int PARTNER_ROW_SIZE_BYTES = 5;
  const int PARTNER_CONVERSION_ROW_SIZE_BYTES = 20;
  const int PUBLISHER_ROW_BYTES = 5;

  // unionMap[i] = j indicates PID i will point to index j in plaintext data
  // note that j in [0,intersectionSize) rather than [0, unionSize)
  // unionMap[i] = -1 indicates PID i is a dummy row
  std::vector<int32_t> shuffleAndGetUnionMap();

  // runs adapter algorithm to get intsersection map
  std::vector<int32_t> getIntersectionMap(const std::vector<int32_t>& unionMap);

  // Serializes input data into rows of fixed width. Different implementations
  // for publisher and partner
  std::vector<std::vector<unsigned char>> preparePlaintextData(
      const std::vector<int32_t>& unionMap);

  /* Runs data processor algorithm to get intersected secret share data
   * intersectionMap is the map of other player. First element is publisher
   * metadata shares, second is partner metadata shares
   */
  std::pair<SecString, SecString> compactData(
      const std::vector<int32_t>& intersectionMap,
      const std::vector<std::vector<unsigned char>>& plaintextData);

  // deserializes the compacted data into MPC structured values
  void extractCompactedData(
      const SecString& publisherDataShares,
      const SecString& partnerDataShares);

  std::tuple<
      std::vector<PartnerRow>,
      std::vector<std::vector<PartnerConversionRow>>,
      std::vector<PublisherRow>>
  deserializeSecretSharedData(
      const SecString& publisherDataShares,
      const SecString& partnerDataShares);

  void extractPartnerValues(const std::vector<PartnerRow>& partnerRows);

  void extractPartnerConversionValues(
      const std::vector<std::vector<PartnerConversionRow>>&
          partnerConversionRows);

  void extractPublisherValues(const std::vector<PublisherRow>& publisherRows);

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

  std::vector<unsigned char> convertFromBits(std::vector<bool> data) {
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

  int32_t myRole_;

  std::unique_ptr<fbpcf::mpc_std_lib::unified_data_process::adapter::IAdapter>
      adapter_;
  std::unique_ptr<fbpcf::mpc_std_lib::unified_data_process::data_processor::
                      IDataProcessor<schedulerId>>
      dataProcessor_;
  std::unique_ptr<fbpcf::engine::util::IPrg> prg_;
  InputData inputData_;
  int32_t numConversionsPerUser_;

  SecBit<schedulerId> controlPopulation_;
  SecGroup<schedulerId> cohortGroupIds_;
  SecBit<schedulerId> breakdownGroupIds_;
  SecGroup<schedulerId> testGroupIds_;

  LiftGameProcessedData<schedulerId> liftGameProcessedData_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor_impl.h"
