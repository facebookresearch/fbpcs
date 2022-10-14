/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/metadata_compaction/IMetadataCompactorGame.h"

namespace private_lift {

template <int schedulerId>
class MetadataCompactorGame : public IMetadataCompactorGame<schedulerId>,
                              public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  MetadataCompactorGame<schedulerId>(
      const int party,
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)),
        party_{party} {}

  std::unique_ptr<IInputProcessor<schedulerId>> play(
      InputData inputData,
      int32_t numConversionPerUser) override {
    throw std::runtime_error("Not implemented");
  }

 private:
  const int party_;
};

} // namespace private_lift
