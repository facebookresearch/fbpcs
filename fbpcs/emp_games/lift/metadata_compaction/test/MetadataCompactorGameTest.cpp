/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <memory>

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/scheduler/ISchedulerFactory.h"
#include "fbpcf/scheduler/LazySchedulerFactory.h"
#include "fbpcf/scheduler/NetworkPlaintextSchedulerFactory.h"

#include "fbpcs/emp_games/lift/metadata_compaction/MetadataCompactorGameFactory.h"

namespace private_lift {

template <int schedulerId>
std::unique_ptr<IMetadataCompactorGame<schedulerId>> createCompactorGame(
    int myId,
    int useXorEncryption,
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        agentFactory) {
  auto scheduler = useXorEncryption
      ? fbpcf::scheduler::getLazySchedulerFactoryWithRealEngine(
            myId, *agentFactory)
            ->create()
      : fbpcf::scheduler::NetworkPlaintextSchedulerFactory<false>(
            myId, *agentFactory)
            .create();

  auto metadataCompactorGameFactory =
      std::make_unique<MetadataCompactorGameFactory<schedulerId>>(
          std::move(agentFactory));

  return metadataCompactorGameFactory->create(std::move(scheduler), myId);
}

class MetadataCompactorGameTestFixture : public ::testing::TestWithParam<bool> {
 protected:
  std::unique_ptr<MetadataCompactorGame<0>> compactorGame0;
  std::unique_ptr<MetadataCompactorGame<1>> compactorGame1;

  void SetUp() override {
    auto factories = fbpcf::engine::communication::getInMemoryAgentFactory(2);

    auto future0 = std::async(
        createCompactorGame<0>, 0, GetParam(), std::move(factories[0]));
    auto future1 = std::async(
        createCompactorGame<1>, 1, GetParam(), std::move(factories[1]));

    auto compactorGame0_2 = future0.get();
    auto compactorGame1_2 = future1.get();
  }
};

TEST_P(MetadataCompactorGameTestFixture, testCreation) {}

INSTANTIATE_TEST_SUITE_P(
    MetadataCompactorGameTest,
    MetadataCompactorGameTestFixture,
    ::testing::Bool(),
    [](const testing::TestParamInfo<
        MetadataCompactorGameTestFixture::ParamType>& info) {
      std::string useXorEncryption = info.param ? "True" : "False";

      std::string name = "UseXor_" + useXorEncryption;
      return name;
    });
} // namespace private_lift
