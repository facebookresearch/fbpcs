/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "folly/logging/xlog.h"

#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/IDataProcessor.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Util.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/Constants.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/IInputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"

#include "fbpcf/mpc_std_lib/unified_data_process/serialization/IRowStructureDefinition.h"
#include "fbpcf/mpc_std_lib/unified_data_process/serialization/RowStructureDefinition.h"

namespace private_lift::input_processing {

template <int schedulerId>
std::unique_ptr<fbpcf::mpc_std_lib::unified_data_process::serialization::
                    IRowStructureDefinition<schedulerId>>
createPublisherSerializer(size_t numConversionsPerUser) {
  using SupportedColumnTypes =
      typename fbpcf::mpc_std_lib::unified_data_process::serialization::
          IColumnDefinition<schedulerId>::SupportedColumnTypes;

  const std::map<std::string, SupportedColumnTypes> publisherRowDefinition{
      {"breakdownId", SupportedColumnTypes::Bit},
      {"controlPopulation", SupportedColumnTypes::Bit},
      {"isValidOpportunityTimestamp", SupportedColumnTypes::Bit},
      {"testReach", SupportedColumnTypes::Bit},
      {"opportunityTimestamp", SupportedColumnTypes::UInt32},
  };

  return std::make_unique<
      fbpcf::mpc_std_lib::unified_data_process::serialization::
          RowStructureDefinition<schedulerId>>(
      publisherRowDefinition, numConversionsPerUser);
}

template <int schedulerId>
std::unique_ptr<fbpcf::mpc_std_lib::unified_data_process::serialization::
                    IRowStructureDefinition<schedulerId>>
createPartnerSerializer(size_t numConversionsPerUser) {
  using SupportedColumnTypes =
      typename fbpcf::mpc_std_lib::unified_data_process::serialization::
          IColumnDefinition<schedulerId>::SupportedColumnTypes;

  const std::map<std::string, SupportedColumnTypes> partnerRowDefinition{
      {"anyValidPurchaseTimestamp", SupportedColumnTypes::Bit},
      {"cohortGroupId", SupportedColumnTypes::UInt32},
      {"purchaseTimestamp", SupportedColumnTypes::UInt32Vec},
      {"thresholdTimestamp", SupportedColumnTypes::UInt32Vec},
      {"purchaseValue", SupportedColumnTypes::Int32Vec},
      {"purchaseValueSquared", SupportedColumnTypes::Int64Vec},
  };

  return std::make_unique<
      fbpcf::mpc_std_lib::unified_data_process::serialization::
          RowStructureDefinition<schedulerId>>(
      partnerRowDefinition, numConversionsPerUser);
}

template <int schedulerId>
using SecString = typename fbpcf::mpc_std_lib::unified_data_process::
    data_processor::IDataProcessor<schedulerId>::SecString;

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

  auto publisherSerializer =
      input_processing::createPublisherSerializer<schedulerId>(
          numConversionsPerUser);

  auto partnerSerializer =
      input_processing::createPartnerSerializer<schedulerId>(
          numConversionsPerUser);

  auto publisherDeserialized =
      publisherSerializer->deserializeUDPOutputIntoMPCTypes(
          publisherDataShares);
  auto partnerDeserialized =
      partnerSerializer->deserializeUDPOutputIntoMPCTypes(partnerDataShares);

  using MPCTypes = fbpcf::frontend::MPCTypes<schedulerId, true>;

  breakdownGroupIds = std::get<typename MPCTypes::SecBool>(
      publisherDeserialized.at("breakdownId"));
  controlPopulation = std::get<typename MPCTypes::SecBool>(
      publisherDeserialized.at("controlPopulation"));
  cohortGroupIds = std::get<typename MPCTypes::SecUnsigned32Int>(
      partnerDeserialized.at("cohortGroupId"));

  liftGameProcessedData.isValidOpportunityTimestamp =
      std::get<typename MPCTypes::SecBool>(
          publisherDeserialized.at("isValidOpportunityTimestamp"));
  liftGameProcessedData.testReach = std::get<typename MPCTypes::SecBool>(
      publisherDeserialized.at("testReach"));
  liftGameProcessedData.opportunityTimestamps =
      std::get<typename MPCTypes::SecUnsigned32Int>(
          publisherDeserialized.at("opportunityTimestamp"));

  liftGameProcessedData.anyValidPurchaseTimestamp =
      std::get<typename MPCTypes::SecBool>(
          partnerDeserialized.at("anyValidPurchaseTimestamp"));
  liftGameProcessedData.purchaseTimestamps =
      std::get<std::vector<typename MPCTypes::SecUnsigned32Int>>(
          partnerDeserialized.at("purchaseTimestamp"));
  liftGameProcessedData.thresholdTimestamps =
      std::get<std::vector<typename MPCTypes::SecUnsigned32Int>>(
          partnerDeserialized.at("thresholdTimestamp"));
  liftGameProcessedData.purchaseValues =
      std::get<std::vector<typename MPCTypes::Sec32Int>>(
          partnerDeserialized.at("purchaseValue"));
  liftGameProcessedData.purchaseValueSquared =
      std::get<std::vector<typename MPCTypes::Sec64Int>>(
          partnerDeserialized.at("purchaseValueSquared"));
}

} // namespace private_lift::input_processing
