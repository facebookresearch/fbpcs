/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/engine/util/AesPrgFactory.h"
#include "fbpcf/frontend/mpcGame.h"
#include "fbpcf/mpc_std_lib/compactor/DummyCompactor.h"
#include "fbpcf/mpc_std_lib/compactor/DummyCompactorFactory.h"
#include "fbpcf/mpc_std_lib/compactor/ICompactor.h"
#include "fbpcf/mpc_std_lib/compactor/ICompactorFactory.h"
#include "fbpcf/mpc_std_lib/compactor/ShuffleBasedCompactor.h"
#include "fbpcf/mpc_std_lib/compactor/ShuffleBasedCompactorFactory.h"
#include "fbpcf/mpc_std_lib/permuter/AsWaksmanPermuterFactory.h"
#include "fbpcf/mpc_std_lib/shuffler/NonShufflerFactory.h"
#include "fbpcf/mpc_std_lib/shuffler/PermuteBasedShufflerFactory.h"
#include "fbpcf/mpc_std_lib/util/util.h"
#include "fbpcf/scheduler/IScheduler.h"
#include "fbpcf/scheduler/SchedulerHelper.h"
#include "fbpcs/emp_games/compactor/AttributionOutput.h"

namespace compactor {

template <typename T, int schedulerId>
class BaseCompactorGame : public fbpcf::frontend::MpcGame<schedulerId> {
 public:
  BaseCompactorGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      int myId,
      int partnerId)
      : fbpcf::frontend::MpcGame<schedulerId>(std::move(scheduler)),
        myId_(myId),
        partnerId_(partnerId) {}

  virtual ~BaseCompactorGame() = default;

  SecretAttributionOutput<schedulerId> play(
      const SecretAttributionOutput<schedulerId>& secret,
      size_t size,
      bool shouldRevealSize) {
    auto compactor = getCompactor(myId_, partnerId_);
    auto [rstMetadata, rstLabel] = compactor->compaction(
        {secret.adId, secret.conversionValue},
        secret.isAttributed,
        size,
        shouldRevealSize);
    SecretAttributionOutput<schedulerId> rst;
    rst.adId = rstMetadata.first;
    rst.conversionValue = rstMetadata.second;
    rst.isAttributed = rstLabel;
    return rst;
  }

 private:
  virtual std::unique_ptr<fbpcf::mpc_std_lib::compactor::ICompactor<
      typename fbpcf::mpc_std_lib::util::SecBatchType<T, schedulerId>::type,
      typename fbpcf::mpc_std_lib::util::SecBatchType<bool, schedulerId>::type>>
  getCompactor(int myId, int partnerId) = 0;
  int myId_;
  int partnerId_;
};

template <typename T, int schedulerId>
class ShuffleBasedCompactorGame : public BaseCompactorGame<T, schedulerId> {
 public:
  ShuffleBasedCompactorGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      int myId,
      int partnerId)
      : BaseCompactorGame<T, schedulerId>(
            std::move(scheduler),
            myId,
            partnerId) {}

 private:
  std::unique_ptr<fbpcf::mpc_std_lib::compactor::ICompactor<
      typename fbpcf::mpc_std_lib::util::SecBatchType<T, schedulerId>::type,
      typename fbpcf::mpc_std_lib::util::SecBatchType<bool, schedulerId>::type>>
  getCompactor(int myId, int partnerId) override {
    return std::make_unique<
               fbpcf::mpc_std_lib::compactor::
                   ShuffleBasedCompactorFactory<T, bool, schedulerId>>(
               myId,
               partnerId,
               std::make_unique<
                   fbpcf::mpc_std_lib::shuffler::PermuteBasedShufflerFactory<
                       std::pair<
                           typename fbpcf::mpc_std_lib::util::
                               SecBatchType<T, schedulerId>::type,
                           typename fbpcf::mpc_std_lib::util::
                               SecBatchType<bool, schedulerId>::type>>>(
                   myId,
                   partnerId,
                   std::make_unique<
                       fbpcf::mpc_std_lib::permuter::AsWaksmanPermuterFactory<
                           std::pair<T, bool>,
                           schedulerId>>(myId, partnerId),
                   std::make_unique<fbpcf::engine::util::AesPrgFactory>()))
        ->create();
  }
};

template <typename T, int schedulerId>
class NonShuffleBasedCompactorGame : public BaseCompactorGame<T, schedulerId> {
 public:
  NonShuffleBasedCompactorGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      int myId,
      int partnerId)
      : BaseCompactorGame<T, schedulerId>(
            std::move(scheduler),
            myId,
            partnerId) {}

 private:
  std::unique_ptr<fbpcf::mpc_std_lib::compactor::ICompactor<
      typename fbpcf::mpc_std_lib::util::SecBatchType<T, schedulerId>::type,
      typename fbpcf::mpc_std_lib::util::SecBatchType<bool, schedulerId>::type>>
  getCompactor(int myId, int partnerId) override {
    return std::make_unique<
               fbpcf::mpc_std_lib::compactor::
                   ShuffleBasedCompactorFactory<T, bool, schedulerId>>(
               myId,
               partnerId,
               std::make_unique<
                   fbpcf::mpc_std_lib::shuffler::insecure::NonShufflerFactory<
                       std::pair<
                           typename fbpcf::mpc_std_lib::util::
                               SecBatchType<T, schedulerId>::type,
                           typename fbpcf::mpc_std_lib::util::
                               SecBatchType<bool, schedulerId>::type>>>())
        ->create();
  }
};

template <typename T, int schedulerId>
class DummyCompactorGame : public BaseCompactorGame<T, schedulerId> {
 public:
  DummyCompactorGame(
      std::unique_ptr<fbpcf::scheduler::IScheduler> scheduler,
      int myId,
      int partnerId)
      : BaseCompactorGame<T, schedulerId>(
            std::move(scheduler),
            myId,
            partnerId) {}

 private:
  std::unique_ptr<fbpcf::mpc_std_lib::compactor::ICompactor<
      typename fbpcf::mpc_std_lib::util::SecBatchType<T, schedulerId>::type,
      typename fbpcf::mpc_std_lib::util::SecBatchType<bool, schedulerId>::type>>
  getCompactor(int myId, int partnerId) override {
    return std::make_unique<fbpcf::mpc_std_lib::compactor::insecure::
                                DummyCompactorFactory<T, bool, schedulerId>>(
               myId, partnerId)
        ->create();
  }
};
} // namespace compactor
