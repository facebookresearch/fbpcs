/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
#include "fbpcf/engine/util/IPrg.h"
#include "fbpcs/data_processing/unified_data_process/adapter/IAdapter.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/IDataProcessor.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
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
  using SecString =
      typename unified_data_process::data_processor::IDataProcessor<
          schedulerId>::SecString;

  CompactionBasedInputProcessor(
      int myRole,
      std::unique_ptr<unified_data_process::adapter::IAdapter> adapter,
      std::unique_ptr<
          unified_data_process::data_processor::IDataProcessor<schedulerId>>
          dataProcessor,
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
      throw std::invalid_argument("Tried to process dataset with no rows");
    }

    auto unionMap = shuffleAndGetUnionMap();
    auto intersectionMap = getIntersectionMap(unionMap);

    auto plaintextData = preparePlaintextData(unionMap);

    auto publisherPartnerJointMetadataShares =
        compactData(intersectionMap, plaintextData);

    extractCompactedData(
        std::get<0>(publisherPartnerJointMetadataShares),
        std::get<1>(publisherPartnerJointMetadataShares));
  }

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

  int32_t myRole_;

  std::unique_ptr<unified_data_process::adapter::IAdapter> adapter_;
  std::unique_ptr<
      unified_data_process::data_processor::IDataProcessor<schedulerId>>
      dataProcessor_;
  std::unique_ptr<fbpcf::engine::util::IPrg> prg_;
  InputData inputData_;
  int32_t numConversionsPerUser_;

  LiftGameProcessedData<schedulerId> liftGameProcessedData_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor_impl.h"
