/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
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
      InputData inputData,
      int32_t numConversionsPerUser)
      : myRole_{myRole},
        adapter_{std::move(adapter)},
        dataProcessor_{std::move(dataProcessor)},
        inputData_{inputData},
        numConversionsPerUser_{numConversionsPerUser} {
    if (inputData.getNumRows() == 0) {
      throw std::invalid_argument("Tried to process dataset with no rows");
    }

    auto unionMap = shuffleAndGetUnionMap();
    auto intersectionMap = getIntersectionMap(unionMap);

    auto plaintextData = preparePlaintextData();

    compactData(intersectionMap, plaintextData);
    extractCompactedData();
  }

  const LiftGameProcessedData<schedulerId>& getLiftGameProcessedData()
      const override {
    return liftGameProcessedData_;
  }

 private:
  // shuffles the input data and returns the union map
  std::vector<int32_t> shuffleAndGetUnionMap();

  // runs adapter algorithm to get intsersection map
  std::vector<int32_t> getIntersectionMap(const std::vector<int32_t>& unionMap);

  // Serializes input data into rows of fixed width. Different implementations
  // for publisher and partner
  std::vector<std::vector<unsigned char>> preparePlaintextData();

  /* Runs data processor algorithm to get intersected secret share data
   * intersectionMap is the map of other player. Results are stored in
   * publisherDataShares_ and partnerDataShares_
   */
  void compactData(
      const std::vector<int32_t>& intersectionMap,
      const std::vector<std::vector<unsigned char>>& plaintextData);

  // deserializes the compacted data into MPC structured values
  void extractCompactedData();

  int32_t myRole_;

  std::unique_ptr<unified_data_process::adapter::IAdapter> adapter_;
  std::unique_ptr<
      unified_data_process::data_processor::IDataProcessor<schedulerId>>
      dataProcessor_;

  InputData inputData_;
  int32_t numConversionsPerUser_;

  SecString publisherDataShares_;
  SecString partnerDataShares_;

  LiftGameProcessedData<schedulerId> liftGameProcessedData_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/CompactionBasedInputProcessor_impl.h"
