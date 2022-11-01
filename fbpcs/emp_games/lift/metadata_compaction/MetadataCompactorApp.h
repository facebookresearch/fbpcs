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
      std::shared_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory,
      std::unique_ptr<IMetadataCompactorGameFactory<schedulerId>>
          compactorGameFactory,
      int numConversionsPerUser,
      bool computePublisherBreakdowns,
      int epoch,
      const std::vector<std::string>& inputPaths,
      const std::vector<std::string>& outputGlobalParamsPaths,
      const std::vector<std::string>& outputSecretSharesPaths,
      int startFileIndex,
      int numFiles,
      bool useXorEncryption = true)
      : party_{party},
        communicationAgentFactory_{std::move(communicationAgentFactory)},
        compactorGameFactory_{std::move(compactorGameFactory)},
        numConversionsPerUser_{numConversionsPerUser},
        computePublisherBreakdowns_{computePublisherBreakdowns},
        epoch_{epoch},
        inputPaths_{inputPaths},
        outputGlobalParamsPaths_{outputGlobalParamsPaths},
        outputSecretSharesPaths_{outputSecretSharesPaths},
        startFileIndex_{startFileIndex},
        numFiles_{numFiles},
        useXorEncryption_{useXorEncryption} {}

  void run();

  common::SchedulerStatistics getSchedulerStatistics() {
    return schedulerStatistics_;
  }

 protected:
  InputData getInputData(const std::string& inputPath);

  std::unique_ptr<fbpcf::scheduler::IScheduler> createScheduler();

 private:
  int party_;
  std::function<std::unique_ptr<IMetadataCompactorGame<schedulerId>>(
      std::unique_ptr<fbpcf::scheduler::IScheduler>,
      int)>
      metadataCompactorGameCreator_;
  std::shared_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  std::unique_ptr<IMetadataCompactorGameFactory<schedulerId>>
      compactorGameFactory_;
  int numConversionsPerUser_;
  bool computePublisherBreakdowns_;
  int epoch_;
  std::vector<std::string> inputPaths_;
  std::vector<std::string> outputGlobalParamsPaths_;
  std::vector<std::string> outputSecretSharesPaths_;
  int startFileIndex_;
  int numFiles_;
  bool useXorEncryption_;
  common::SchedulerStatistics schedulerStatistics_;
};

} // namespace private_lift

#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorApp_impl.h"
