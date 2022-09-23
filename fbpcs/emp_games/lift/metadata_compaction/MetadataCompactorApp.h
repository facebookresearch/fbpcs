/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/scheduler/IScheduler.h"
#include "fbpcs/emp_games/common/SchedulerStatistics.h"
#include "fbpcs/emp_games/lift/metadata_compaction/IMetadataCompactorGame.h"
#include "fbpcs/emp_games/lift/metadata_compaction/IMetadataCompactorGameFactory.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/input_processing/InputData.h"

namespace private_lift {

template <int schedulerId>
class MetadataCompactorApp {
 public:
  MetadataCompactorApp(
      int party,
      std::unique_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      std::unique_ptr<IMetadataCompactorGameFactory<schedulerId>>
          compactorGameFactory,
      int numConversionsPerUser,
      bool computePublisherBreakdowns,
      int epoch,
      const std::string& inputPath,
      const std::string& outputGlobalParamsPath,
      const std::string& outputSecretSharesPath,
      bool useXorEncryption = true)
      : party_{party},
        communicationAgentFactory_{std::move(communicationAgentFactory)},
        compactorGameFactory_{std::move(compactorGameFactory)},
        numConversionsPerUser_{numConversionsPerUser},
        computePublisherBreakdowns_{computePublisherBreakdowns},
        epoch_{epoch},
        inputPath_{inputPath},
        outputGlobalParamsPath_{outputGlobalParamsPath},
        outputSecretSharesPath_{outputSecretSharesPath},
        useXorEncryption_{useXorEncryption} {}

  void run();

 protected:
  std::unique_ptr<fbpcf::scheduler::IScheduler> createScheduler();

 private:
  int party_;
  std::function<std::unique_ptr<IMetadataCompactorGame<schedulerId>>(
      std::unique_ptr<fbpcf::scheduler::IScheduler>,
      int)>
      metadataCompactorGameCreator_;
  std::unique_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  std::unique_ptr<IMetadataCompactorGameFactory<schedulerId>>
      compactorGameFactory_;
  int numConversionsPerUser_;
  bool computePublisherBreakdowns_;
  int epoch_;
  std::string inputPath_;
  std::string outputGlobalParamsPath_;
  std::string outputSecretSharesPath_;
  bool useXorEncryption_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorApp_impl.h"
