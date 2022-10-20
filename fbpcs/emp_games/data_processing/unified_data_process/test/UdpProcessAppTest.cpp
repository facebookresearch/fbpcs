/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include "folly/Format.h"
#include "folly/Random.h"

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/scheduler/ISchedulerFactory.h"
#include "fbpcf/test/TestHelper.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessApp.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessGameFactory.h"
#include "fbpcs/performance_tools/CostEstimation.h"

namespace unified_data_process {
template <int schedulerId>
std::tuple<std::vector<std::vector<bool>>, std::vector<std::vector<bool>>>
runUdpProcessApp(
    int myId,
    int32_t rowNumber,
    int32_t rowSize,
    int32_t intersectionSize,
    std::shared_ptr<fbpcs::performance_tools::CostEstimation> costEst,
    std::shared_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory>
        communicationAgentFactory,
    std::shared_ptr<fbpcf::util::MetricCollector> metricCollector,
    std::unique_ptr<UdpProcessGameFactory<schedulerId>> udpGameFactory) {
  UdpProcessApp<schedulerId> app(
      myId,
      std::move(communicationAgentFactory),
      std::move(metricCollector),
      std::move(udpGameFactory),
      rowNumber,
      rowSize,
      intersectionSize,
      costEst);
  return app.run();
}

std::vector<std::vector<uint8_t>> reconstructResults(
    std::vector<std::vector<bool>>& booleanShares0,
    std::vector<std::vector<bool>>& booleanShares1) {
  std::vector<std::vector<bool>> booleanShares(
      booleanShares0.size(), std::vector<bool>(booleanShares0.at(0).size()));
  for (size_t i = 0; i < booleanShares0.size(); ++i) {
    for (size_t j = 0; j < booleanShares0.at(0).size(); ++j) {
      booleanShares[i][j] = booleanShares0[i][j] ^ booleanShares1[i][j];
    }
  }
  std::vector<std::vector<uint8_t>> reconstructedData(
      booleanShares.at(0).size(),
      std::vector<uint8_t>(booleanShares.size() / 8));

  for (size_t i = 0; i < booleanShares.size() / 8; i++) {
    for (uint8_t j = 0; j < 8; j++) {
      for (size_t k = 0; k < booleanShares.at(0).size(); k++) {
        reconstructedData[k][i] += (booleanShares.at(i * 8 + j).at(k) << j);
      }
    }
  }

  return reconstructedData;
}

void checkOutput(
    const std::vector<std::vector<uint8_t>>& publisherData,
    const std::vector<std::vector<uint8_t>>& partnerData,
    int32_t row_size,
    int32_t intersectionSize) {
  ASSERT_EQ(publisherData.size(), intersectionSize);
  ASSERT_EQ(partnerData.size(), intersectionSize);
  ASSERT_EQ(publisherData.at(0).size(), row_size);
  ASSERT_EQ(partnerData.at(0).size(), row_size);

  // The intersected meta data on both
  // party were set to be the same for the ease of
  // correctness verification.
  EXPECT_EQ(publisherData, partnerData);
}

TEST(UdpProcessApp, testUdpProcessApp) {
  std::random_device rd;
  std::mt19937_64 e(rd());
  std::uniform_int_distribution<int32_t> randomRowNum(100, 0xFF);
  std::uniform_int_distribution<int32_t> randomRowSize(64, 80);
  std::uniform_int_distribution<uint8_t> randomRate(1, 20);
  int32_t rowNumber = randomRowNum(e);
  int32_t rowSize = randomRowSize(e);
  double intersectionRate = randomRate(e);
  int32_t intersectionSize = (intersectionRate / 100) * rowNumber;

  auto agentFactories =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(*agentFactories[0], *agentFactories[1]);
  auto udpGameFactory0 =
      std::make_unique<UdpProcessGameFactory<0>>(0, *agentFactories[0]);
  auto udpGameFactory1 =
      std::make_unique<UdpProcessGameFactory<1>>(1, *agentFactories[1]);

  auto metricCollector0 =
      std::make_shared<fbpcf::util::MetricCollector>("attribution_test_0");

  auto metricCollector1 =
      std::make_shared<fbpcf::util::MetricCollector>("attribution_test_1");

  auto costEst0 = std::make_shared<fbpcs::performance_tools::CostEstimation>(
      "data_processing_udp", "test_bucket", "test_s3_region", "pcf2");
  costEst0->start();
  auto costEst1 = std::make_shared<fbpcs::performance_tools::CostEstimation>(
      "data_processing_udp", "test_bucket", "test_s3_region", "pcf2");
  costEst1->start();

  auto future0 = std::async(
      runUdpProcessApp<0>,
      0,
      rowNumber,
      rowSize,
      intersectionSize,
      costEst0,
      std::move(agentFactories[0]),
      std::move(metricCollector0),
      std::move(udpGameFactory0));
  auto future1 = std::async(
      runUdpProcessApp<1>,
      1,
      rowNumber,
      rowSize,
      intersectionSize,
      costEst1,
      std::move(agentFactories[1]),
      std::move(metricCollector1),
      std::move(udpGameFactory1));

  auto sharesOutput0 = future0.get();
  auto sharesOutput1 = future1.get();

  auto& publisherDataShares0 = std::get<0>(sharesOutput0);
  auto& partnerDataShares0 = std::get<1>(sharesOutput0);

  auto& publisherDataShares1 = std::get<0>(sharesOutput1);
  auto& partnerDataShares1 = std::get<1>(sharesOutput1);

  auto publisherData =
      reconstructResults(publisherDataShares0, publisherDataShares1);
  auto partnerData = reconstructResults(partnerDataShares0, partnerDataShares1);
  checkOutput(publisherData, partnerData, rowSize, intersectionSize);
}

} // namespace unified_data_process
