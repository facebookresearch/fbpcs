/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdexcept>
#include <vector>
#include "fbpcf/engine/util/IPrg.h"
#include "folly/logging/xlog.h"

#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/UdpDecryption.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpDecryptor/UdpDecryptorApp.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/GlobalSharingUtils.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/LiftCompactionUtils.h"
namespace private_lift {
/**
 * This class handles the deserialization of serialized metadata produced from
 * UDP.
 */
template <int schedulerId>
class DecoupledUDPInputProcessor : public IInputProcessor<schedulerId> {
 public:
  using SecString = typename fbpcf::mpc_std_lib::unified_data_process::
      data_processor::IDataProcessor<schedulerId>::SecString;

  DecoupledUDPInputProcessor(
      int myRole,
      const std::string& inputGlobalParamsPath,
      const std::string& inputExpandedKeyPath,
      const std::string& inputCiphertextsPath,
      int32_t numConversionsPerUser)
      : myRole_{myRole}, numConversionsPerUser_{numConversionsPerUser} {
    // Run UDP decryption
    unified_data_process::UdpDecryptorApp<schedulerId> decryptionApp{
        std::make_unique<fbpcf::mpc_std_lib::unified_data_process::
                             data_processor::UdpDecryption<schedulerId>>(
            myRole, 1 - myRole),
        myRole == common::PUBLISHER};
    std::tuple<SecString, SecString> publisherPartnerJointMetadataShares =
        decryptionApp.invokeUdpDecryption(
            inputCiphertextsPath, inputExpandedKeyPath, inputGlobalParamsPath);

    liftGameProcessedData_.numRows =
        std::get<0>(publisherPartnerJointMetadataShares).size();

    XLOG(INFO, "Begin extraction to MPC types");
    auto publisherShares = std::get<0>(publisherPartnerJointMetadataShares);
    auto partnerShares = std::get<1>(publisherPartnerJointMetadataShares);
    input_processing::extractCompactedData(
        liftGameProcessedData_,
        controlPopulation_,
        cohortGroupIds_,
        breakdownBitGroupIds_,
        publisherShares,
        partnerShares,
        numConversionsPerUser_);

    input_processing::computeIndexSharesAndSetTestGroupIds(
        liftGameProcessedData_,
        cohortGroupIds_,
        controlPopulation_,
        breakdownBitGroupIds_,
        testGroupIds_);
    input_processing::computeTestIndexShares(
        liftGameProcessedData_, controlPopulation_, testGroupIds_);
  }

  const LiftGameProcessedData<schedulerId>& getLiftGameProcessedData()
      const override {
    return liftGameProcessedData_;
  }

 private:
  int32_t myRole_;

  int32_t numConversionsPerUser_;

  SecBit<schedulerId> controlPopulation_;
  SecGroup<schedulerId> cohortGroupIds_;
  SecBit<schedulerId> breakdownBitGroupIds_;
  SecGroup<schedulerId> testGroupIds_;

  LiftGameProcessedData<schedulerId> liftGameProcessedData_;
};
} // namespace private_lift
