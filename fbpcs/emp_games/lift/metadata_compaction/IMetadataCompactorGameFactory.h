/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/scheduler/IScheduler.h"
#include "fbpcs/emp_games/lift/metadata_compaction/IMetadataCompactorGame.h"

namespace private_lift {

template <int schedulerId>
class IMetadataCompactorGameFactory {
 public:
  virtual ~IMetadataCompactorGameFactory() = default;

  virtual std::unique_ptr<IMetadataCompactorGame<schedulerId>> create(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      int partyId) = 0;

 private:
};

} // namespace private_lift
