/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptor.h"
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/test/UdpEncryptionMock.h"

#include <gtest/gtest.h>
#include <functional>
#include <memory>
#include <optional>
#include "folly/Random.h"

using namespace ::testing;
namespace unified_data_process {

TEST(UdpEncryptorTestWithMock, testProcessingPeerData) {
  int chunkSize = 500;
  int totalRow = 1200;
  size_t dataWidth = 32;
  std::vector<uint64_t> indexes{3, 31, 6, 12, 5};
  auto mock = std::make_unique<fbpcf::mpc_std_lib::unified_data_process::
                                   data_processor::UdpEncryptionMock>();

  EXPECT_CALL(*mock, prepareToProcessPeerData(dataWidth, indexes)).Times(1);
  EXPECT_CALL(*mock, processPeerData(chunkSize)).Times(totalRow / chunkSize);
  if (totalRow % chunkSize != 0) {
    EXPECT_CALL(*mock, processPeerData(totalRow % chunkSize)).Times(1);
  }
  EXPECT_CALL(*mock, getProcessedData()).Times(1);

  UdpEncryptor encryptor(std::move(mock), chunkSize);
  encryptor.setPeerConfig(totalRow, dataWidth, indexes);
  encryptor.getEncryptionResults();
}

TEST(UdpEncryptorTestWithMock, testProcessingMyData) {
  size_t chunkSize = 200;
  size_t sampleSize = 219;
  size_t totalRow = 1200;
  size_t width = 32;

  std::vector<std::vector<std::vector<unsigned char>>> testData;
  std::vector<std::vector<std::vector<unsigned char>>> testData1;
  std::vector<std::vector<uint64_t>> index;
  std::vector<std::vector<uint64_t>> index1;
  uint64_t randomIndex = 0;
  for (size_t i = 0; i < totalRow; i += chunkSize) {
    testData.push_back(std::vector<std::vector<unsigned char>>());
    testData.back().reserve(std::min(chunkSize, totalRow - i));
    index.push_back(std::vector<uint64_t>());
    index.back().reserve(std::min(chunkSize, totalRow - i));

    for (size_t j = 0; (j < chunkSize) && (j + i < totalRow); j++) {
      uint8_t randomChar = folly::Random::rand32(0, 0xff);
      randomIndex +=
          folly::Random::rand32(); // make sure random index is always unique
      testData.back().push_back(std::vector<unsigned char>(width, randomChar));
      index.back().push_back(randomIndex);
      if ((i + j) % sampleSize == 0) {
        testData1.push_back(std::vector<std::vector<unsigned char>>());
        index1.push_back(std::vector<uint64_t>());
      }
      testData1.back().push_back(std::vector<unsigned char>(width, randomChar));
      index1.back().push_back(randomIndex);
    }
  }

  auto mock = std::make_unique<fbpcf::mpc_std_lib::unified_data_process::
                                   data_processor::UdpEncryptionMock>();

  EXPECT_CALL(*mock, prepareToProcessMyData(width)).Times(1);
  for (size_t i = 0; i < testData.size(); i++) {
    EXPECT_CALL(*mock, processMyData(testData.at(i), index.at(i))).Times(1);
  }
  EXPECT_CALL(*mock, getExpandedKey()).Times(1);

  UdpEncryptor encryptor(std::move(mock), chunkSize);
  for (size_t i = 0; i < testData1.size() / 2; i++) {
    for (size_t j = 0; j < testData1.at(i).size(); j++) {
      encryptor.pushOneLineFromMe(
          std::move(testData1.at(i).at(j)), index1.at(i).at(j));
    }
  }
  for (size_t i = testData1.size() / 2; i < testData1.size(); i++) {
    encryptor.pushLinesFromMe(
        std::move(testData1.at(i)), std::move(index1.at(i)));
  }
  encryptor.getExpandedKey();
}

TEST(UdpEncryptorTestWithMock, testProcessingBothSidesData) {
  size_t chunkSize = 200;
  size_t sampleSize = 219;
  size_t myTotalRow = 1200;
  size_t peerTotalRow = 1500;
  size_t myWidth = 32;
  size_t peerWidth = 35;
  std::vector<uint64_t> indexes{3, 31, 6, 12, 5};

  std::vector<std::vector<std::vector<unsigned char>>> testData;
  std::vector<std::vector<std::vector<unsigned char>>> testData1;
  std::vector<std::vector<uint64_t>> index;
  std::vector<std::vector<uint64_t>> index1;
  uint64_t randomIndex = 0;
  for (size_t i = 0; i < myTotalRow; i += chunkSize) {
    testData.push_back(std::vector<std::vector<unsigned char>>());
    testData.back().reserve(std::min(chunkSize, myTotalRow - i));
    index.push_back(std::vector<uint64_t>());
    index.back().reserve(std::min(chunkSize, myTotalRow - i));

    for (size_t j = 0; (j < chunkSize) && (j + i < myTotalRow); j++) {
      uint8_t randomChar = folly::Random::rand32(0, 0xff);
      randomIndex +=
          folly::Random::rand32(); // make sure random index is always unique
      testData.back().push_back(
          std::vector<unsigned char>(myWidth, randomChar));
      index.back().push_back(randomIndex);
      if ((i + j) % sampleSize == 0) {
        testData1.push_back(std::vector<std::vector<unsigned char>>());
        index1.push_back(std::vector<uint64_t>());
      }
      testData1.back().push_back(
          std::vector<unsigned char>(myWidth, randomChar));
      index1.back().push_back(randomIndex);
    }
  }

  auto mock = std::make_unique<fbpcf::mpc_std_lib::unified_data_process::
                                   data_processor::UdpEncryptionMock>();

  EXPECT_CALL(*mock, prepareToProcessMyData(myWidth)).Times(1);
  for (size_t i = 0; i < testData.size(); i++) {
    EXPECT_CALL(*mock, processMyData(testData.at(i), index.at(i))).Times(1);
  }
  EXPECT_CALL(*mock, getExpandedKey()).Times(1);

  EXPECT_CALL(*mock, prepareToProcessPeerData(peerWidth, indexes)).Times(1);
  EXPECT_CALL(*mock, processPeerData(chunkSize))
      .Times(peerTotalRow / chunkSize);
  if (peerTotalRow % chunkSize != 0) {
    EXPECT_CALL(*mock, processPeerData(peerTotalRow % chunkSize)).Times(1);
  }
  EXPECT_CALL(*mock, getProcessedData()).Times(1);

  UdpEncryptor encryptor(std::move(mock), chunkSize);
  encryptor.setPeerConfig(peerTotalRow, peerWidth, indexes);

  for (size_t i = 0; i < testData1.size() / 2; i++) {
    for (size_t j = 0; j < testData1.at(i).size(); j++) {
      encryptor.pushOneLineFromMe(
          std::move(testData1.at(i).at(j)), index1.at(i).at(j));
    }
  }
  for (size_t i = testData1.size() / 2; i < testData1.size(); i++) {
    encryptor.pushLinesFromMe(
        std::move(testData1.at(i)), std::move(index1.at(i)));
  }
  encryptor.getExpandedKey();
  encryptor.getEncryptionResults();
}

} // namespace unified_data_process
