/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/metadata_compaction/IMetadataCompactorGameFactory.h"
#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorGame.h"

namespace private_lift {

template <int schedulerId>
class MetadataCompactorGameFactory
    : public IMetadataCompactorGameFactory<schedulerId> {
 public:
  explicit MetadataCompactorGameFactory<schedulerId>(
      std::shared_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          factory)
      : factory_(std::move(factory)) {}

  std::unique_ptr<IMetadataCompactorGame<schedulerId>> create(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      int partyId) {
    return std::make_unique<MetadataCompactorGame<schedulerId>>(
        partyId, std::move(scheduler), *factory_);
  }

 private:
  std::shared_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      factory_;
};

} // namespace private_lift
