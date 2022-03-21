/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cmath>
#include <future>
#include <memory>
#include <random>
#include <unordered_map>

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/scheduler/SchedulerHelper.h"
#include "fbpcf/test/TestHelper.h"
#include "fbpcs/data_processing/unified_data_process/data_processor/DummyDataProcessorFactory.h"

namespace unified_data_process::data_processor {

std::tuple<
    std::vector<std::vector<uint8_t>>,
    std::vector<int64_t>,
    std::vector<std::vector<uint8_t>>>
generateDataProcessorTestData() {
  std::random_device rd;
  std::mt19937_64 e(rd());
  std::uniform_int_distribution<int32_t> randomSize(10, 0xFF);
  std::uniform_int_distribution<uint8_t> randomData(0, 0xFF);
  auto outputSize = randomSize(e);
  auto inputSize = outputSize + randomSize(e);

  size_t dataWidth = 20;
  std::vector<std::vector<uint8_t>> inputData(
      inputSize, std::vector<uint8_t>(dataWidth));

  for (auto& item : inputData) {
    for (auto& data : item) {
      data = randomData(e);
    }
  }
  std::vector<int64_t> index(inputSize);
  for (size_t i = 0; i < inputSize; i++) {
    index[i] = i;
  }
  std::random_shuffle(index.begin(), index.end());
  index.erase(index.begin() + outputSize, index.end());

  std::vector<std::vector<uint8_t>> expectedOutput(outputSize);
  for (size_t i = 0; i < outputSize; i++) {
    expectedOutput[i] = inputData.at(index.at(i));
  }
  return {inputData, index, expectedOutput};
}

void testDataProcessor(
    std::unique_ptr<IDataProcessor<0>> processor0,
    std::unique_ptr<IDataProcessor<1>> processor1) {
  auto [data, index, expectedOutput] = generateDataProcessorTestData();
  auto outputSize = index.size();
  auto dataSize = data.size();
  auto dataWidth = data.at(0).size();
  auto task0 = [](std::unique_ptr<IDataProcessor<0>> processor,
                  const std::vector<std::vector<unsigned char>>& plaintextData,
                  size_t outputSize,
                  size_t dataWidth) {
    auto secretSharedOutput =
        processor->processMyData(plaintextData, outputSize);
    auto plaintextOutputBitString =
        secretSharedOutput.openToParty(0).getValue();
    std::vector<std::vector<uint8_t>> rst(
        outputSize, std::vector<uint8_t>(dataWidth));
    for (size_t i = 0; i < dataWidth; i++) {
      for (uint8_t j = 0; j < 8; j++) {
        for (size_t k = 0; k < outputSize; k++) {
          rst[k][i] += (plaintextOutputBitString.at(i * 8 + j).at(k) << j);
        }
      }
    }
    return rst;
  };
  auto task1 = [](std::unique_ptr<IDataProcessor<1>> processor,
                  size_t dataSize,
                  const std::vector<int64_t>& indexes,
                  size_t dataWidth) {
    auto secretSharedOutput =
        processor->processPeersData(dataSize, indexes, dataWidth);
    secretSharedOutput.openToParty(0);
  };

  auto future0 =
      std::async(task0, std::move(processor0), data, outputSize, dataWidth);
  auto future1 =
      std::async(task1, std::move(processor1), dataSize, index, dataWidth);
  future1.get();
  auto rst = future0.get();
  for (size_t i = 0; i < outputSize; i++) {
    fbpcf::testVectorEq(rst.at(i), expectedOutput.at(i));
  }
}

TEST(DummyDataProcessor, testDummyDataProcessor) {
  auto agentFactories =
      fbpcf::engine::communication::getInMemoryAgentFactory(2);
  fbpcf::setupRealBackend<0, 1>(*agentFactories[0], *agentFactories[1]);

  insecure::DummyDataProcessorFactory<0> factory0(0, 1, *agentFactories[0]);
  insecure::DummyDataProcessorFactory<1> factory1(1, 0, *agentFactories[1]);
  testDataProcessor(factory0.create(), factory1.create());
}

} // namespace unified_data_process::data_processor
