/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fbpcf/io/api/BufferedReader.h>
#include <fbpcf/io/api/BufferedWriter.h>
#include <fbpcf/io/api/FileReader.h>
#include <fbpcf/io/api/FileWriter.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>

#include "fbpcf/engine/communication/test/AgentFactoryCreationHelper.h"
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/UdpDecryption.h"
#include "fbpcf/scheduler/ISchedulerFactory.h"
#include "fbpcf/test/TestHelper.h"
#include "fbpcs/emp_games/data_processing/global_parameters/GlobalParameters.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpDecryptor/UdpDecryptorApp.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptor.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptorApp.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessApp.h"
#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessGameFactory.h"
#include "fbpcs/performance_tools/CostEstimation.h"
#include "folly/Format.h"
#include "folly/Random.h"

namespace unified_data_process {

std::vector<std::vector<unsigned char>> generateRandomDataForTest(
    size_t count,
    size_t width) {
  std::vector<std::vector<unsigned char>> rst;
  rst.reserve(count);
  for (size_t i = 0; i < count; i++) {
    std::vector<unsigned char> data(width);
    for (auto& d : data) {
      d = folly::Random::rand32(32, 127);
    }
    rst.push_back(data);
  }
  return rst;
}

std::vector<uint64_t> generateRandomIndex(
    size_t upperBound,
    size_t outputSize) {
  std::vector<uint64_t> rst(outputSize);
  std::map<int32_t, int32_t> swaps;
  for (size_t i = 0; i < outputSize; i++) {
    auto target = folly::Random::secureRand32(i, upperBound);
    swaps.emplace(target, target);
    swaps.emplace(i, i);
    std::swap(swaps.at(i), swaps.at(target));
  }
  for (size_t i = 0; i < outputSize; i++) {
    rst.at(i) = swaps.at(i);
  }
  return rst;
}

void writeDataToFile(
    const std::string& file,
    const std::vector<uint64_t>& indexes,
    const std::vector<std::vector<unsigned char>>& data) {
  if (indexes.size() != data.size()) {
    throw std::invalid_argument("indexes and data have different length.");
  }
  auto writer = std::make_unique<fbpcf::io::BufferedWriter>(
      std::make_unique<fbpcf::io::FileWriter>(file));
  std::string newLine("\n");
  for (size_t i = 0; i < indexes.size(); i++) {
    std::string line;
    line = std::to_string(indexes.at(i)) + ", " +
        std::string(data.at(i).begin(), data.at(i).end());
    writer->writeString(line);
    writer->writeString(newLine);
  }
}

void writeIndexToFile(
    const std::string& file,
    const std::vector<uint64_t>& indexes) {
  auto writer = std::make_unique<fbpcf::io::BufferedWriter>(
      std::make_unique<fbpcf::io::FileWriter>(file));
  std::string header("dummy header");
  std::string newLine("\n");
  writer->writeString(header);
  writer->writeString(newLine);

  for (auto& index : indexes) {
    writer->writeString("dummyName, " + std::to_string(index));
    writer->writeString(newLine);
  }
}

void distributeDataToFiles(
    const std::vector<std::string>& files,
    const std::vector<uint64_t>& indexes,
    const std::vector<std::vector<unsigned char>>& data) {
  for (size_t i = 0; i < files.size(); i++) {
    writeDataToFile(
        files.at(i),
        std::vector<uint64_t>(
            indexes.begin() + i * data.size() / files.size(),
            indexes.begin() + (i + 1) * data.size() / files.size()),
        std::vector<std::vector<unsigned char>>(
            data.begin() + i * data.size() / files.size(),
            data.begin() + (i + 1) * data.size() / files.size()));
  }
}

void distributeIndexesToFiles(
    const std::vector<std::string>& files,
    const std::vector<uint64_t>& indexes) {
  for (size_t i = 0; i < files.size(); i++) {
    writeIndexToFile(
        files.at(i),
        std::vector<uint64_t>(
            indexes.begin() + i * indexes.size() / files.size(),
            indexes.begin() + (i + 1) * indexes.size() / files.size()));
  }
}

struct TestData {
  std::vector<std::vector<unsigned char>> publisherExpectedOutput;
  std::vector<std::vector<unsigned char>> advertiserExpectedOutput;
  std::vector<std::string> publisherIndexFiles;
  std::vector<std::string> advertiserIndexFiles;
  std::vector<std::string> publisherDataFiles;
  std::vector<std::string> advertiserDataFiles;
  std::string globalParameterFile;

  std::vector<std::string> publisherEncryptionFiles;
  std::string publisherExpandedKeyFile;

  std::vector<std::string> advertiserEncryptionFiles;
  std::string advertiserExpandedKeyFile;

  size_t intersectionSize;
};

TestData generateTestData(
    size_t publisherRowCount,
    size_t advertiserRowCount,
    size_t publisherWidth,
    size_t advertiserWidth,
    size_t intersectionSize,
    int publisherFileCount,
    int advertiserFileCount,
    int encryptionFileCount) {
  std::string tempDir = std::filesystem::temp_directory_path();
  const std::string publisherDataPath =
      folly::sformat("{}/publisher_data_{}_", tempDir, folly::Random::rand32());
  const std::string advertiserDataPath = folly::sformat(
      "{}/advertiser_data_{}_", tempDir, folly::Random::rand32());
  const std::string publisherIndexPath = folly::sformat(
      "{}/publisher_index_{}_", tempDir, folly::Random::rand32());
  const std::string advertiserIndexPath = folly::sformat(
      "{}/advertiser_index_{}_", tempDir, folly::Random::rand32());
  const std::string globalParameter = folly::sformat(
      "{}/global_parameters_{}", tempDir, folly::Random::rand32());
  const std::string publisherEncryptionFile = folly::sformat(
      "{}/publisher_Encryption_{}_", tempDir, folly::Random::rand32());
  const std::string publisherExpandedKeyFile = folly::sformat(
      "{}/publisher_ExpandedKey_{}", tempDir, folly::Random::rand32());
  const std::string advertiserEncryptionFile = folly::sformat(
      "{}/advertiser_Encryption_{}_", tempDir, folly::Random::rand32());
  const std::string advertiserExpandedKeyFile = folly::sformat(
      "{}/advertiser_ExpandedKey_{}", tempDir, folly::Random::rand32());

  std::vector<std::string> publisherDataFiles;
  std::vector<std::string> advertiserDataFiles;
  std::vector<std::string> publisherIndexFiles;
  std::vector<std::string> advertiserIndexFiles;
  std::vector<std::string> publisherEncryptionFiles;
  std::vector<std::string> advertiserEncryptionFiles;

  for (size_t i = 0; i < publisherFileCount; i++) {
    publisherDataFiles.push_back(publisherDataPath + std::to_string(i));
    publisherIndexFiles.push_back(publisherIndexPath + std::to_string(i));
  }

  for (size_t i = 0; i < advertiserFileCount; i++) {
    advertiserDataFiles.push_back(advertiserDataPath + std::to_string(i));
    advertiserIndexFiles.push_back(advertiserIndexPath + std::to_string(i));
  }
  for (size_t i = 0; i < encryptionFileCount; i++) {
    advertiserEncryptionFiles.push_back(
        advertiserEncryptionFile + std::to_string(i));
    publisherEncryptionFiles.push_back(
        publisherEncryptionFile + std::to_string(i));
  }

  auto publisherData =
      generateRandomDataForTest(publisherRowCount, publisherWidth);
  auto advertiserData =
      generateRandomDataForTest(advertiserRowCount, advertiserWidth);
  // users are given random indexes for the sake of performance
  // intend to use this piece of code in the end, comment out for now as
  // implementations are not done yet.
  /*
  auto publisherRandomIndexForAllUser =
      generateRandomIndex(publisherRowCount * 100, publisherRowCount);
  auto advertiserRandomIndexForAllUser =
      generateRandomIndex(advertiserRowCount * 200, advertiserRowCount);
  */
  std::vector<uint64_t> publisherRandomIndexForAllUser(publisherRowCount);
  // generate 0 to n-1 vector
  std::iota(
      publisherRandomIndexForAllUser.begin(),
      publisherRandomIndexForAllUser.end(),
      0);

  std::vector<uint64_t> advertiserRandomIndexForAllUser(advertiserRowCount);
  // generate 0 to n-1 vector
  std::iota(
      advertiserRandomIndexForAllUser.begin(),
      advertiserRandomIndexForAllUser.end(),
      0);

  auto publisherActualIndexForMatchedUser =
      generateRandomIndex(publisherRowCount, intersectionSize);
  auto advertiserActualIndexForMatchedUser =
      generateRandomIndex(advertiserRowCount, intersectionSize);

  std::vector<std::vector<unsigned char>> publisherExpectedOutput;
  std::vector<std::vector<unsigned char>> advertiserExpectedOutput;
  std::vector<uint64_t> publisherCherryPickIndex;
  std::vector<uint64_t> advertiserCherryPickIndex;

  for (size_t i = 0; i < intersectionSize; i++) {
    publisherExpectedOutput.push_back(
        publisherData.at(publisherActualIndexForMatchedUser.at(i)));
    advertiserExpectedOutput.push_back(
        advertiserData.at(advertiserActualIndexForMatchedUser.at(i)));
    publisherCherryPickIndex.push_back(advertiserRandomIndexForAllUser.at(
        advertiserActualIndexForMatchedUser.at(i)));
    advertiserCherryPickIndex.push_back(publisherRandomIndexForAllUser.at(
        publisherActualIndexForMatchedUser.at(i)));
  }

  distributeDataToFiles(
      publisherDataFiles, publisherRandomIndexForAllUser, publisherData);
  distributeDataToFiles(
      advertiserDataFiles, advertiserRandomIndexForAllUser, advertiserData);

  distributeIndexesToFiles(publisherIndexFiles, publisherCherryPickIndex);
  distributeIndexesToFiles(advertiserIndexFiles, advertiserCherryPickIndex);

  global_parameters::GlobalParameters gp;
  gp.emplace(global_parameters::KAdvDataWidth, advertiserWidth);
  gp.emplace(global_parameters::KPubDataWidth, publisherWidth);
  gp.emplace(global_parameters::KAdvRowCount, advertiserRowCount);
  gp.emplace(global_parameters::KPubRowCount, publisherRowCount);
  global_parameters::writeToFile(globalParameter, gp);

  return TestData{
      .publisherExpectedOutput = std::move(publisherExpectedOutput),
      .advertiserExpectedOutput = std::move(advertiserExpectedOutput),
      .publisherIndexFiles = std::move(publisherIndexFiles),
      .advertiserIndexFiles = std::move(advertiserIndexFiles),
      .publisherDataFiles = std::move(publisherDataFiles),
      .advertiserDataFiles = std::move(advertiserDataFiles),
      .globalParameterFile = globalParameter,
      .publisherEncryptionFiles = publisherEncryptionFiles,
      .publisherExpandedKeyFile = publisherExpandedKeyFile,
      .advertiserEncryptionFiles = advertiserEncryptionFiles,
      .advertiserExpandedKeyFile = advertiserExpandedKeyFile,
      .intersectionSize = intersectionSize,
  };
}

std::vector<std::vector<uint8_t>> convertToBytes(
    const std::vector<std::vector<bool>>& src,
    size_t dataWidth,
    size_t outputSize) {
  std::vector<std::vector<uint8_t>> rst(
      outputSize, std::vector<uint8_t>(dataWidth));
  for (size_t i = 0; i < dataWidth; i++) {
    for (uint8_t j = 0; j < 8; j++) {
      for (size_t k = 0; k < outputSize; k++) {
        rst[k][i] += (src.at(i * 8 + j).at(k) << j);
      }
    }
  }
  return rst;
}

template <int schedulerId>
std::vector<std::vector<uint8_t>> test(
    std::unique_ptr<
        fbpcf::engine::communication::IPartyCommunicationAgentFactory> factory,
    const std::vector<std::string>& indexFiles,
    const std::vector<std::string>& dataFiles,
    const std::string& parameterFile,
    const std::vector<std::string>& encryptionFiles,
    const std::string& expandedKeyFile,
    size_t intersectionSize) {
  int chunkSize = 5;
  UdpEncryptorApp encryptionApp(
      std::make_unique<UdpEncryptor>(
          std::make_unique<fbpcf::mpc_std_lib::unified_data_process::
                               data_processor::UdpEncryption>(
              factory->create(1 - schedulerId, "test")),
          chunkSize),
      schedulerId == 0);

  encryptionApp.invokeUdpEncryption(
      indexFiles, dataFiles, parameterFile, encryptionFiles, expandedKeyFile);

  UdpDecryptorApp<schedulerId> decryptionApp{
      std::make_unique<fbpcf::mpc_std_lib::unified_data_process::
                           data_processor::UdpDecryption<schedulerId>>(
          schedulerId, 1 - schedulerId),
      schedulerId == 0};
  auto gp = global_parameters::readFromFile(parameterFile);
  auto publisherWidth =
      boost::get<int32_t>(gp.at(global_parameters::KPubDataWidth));
  auto advertiserWidth =
      boost::get<int32_t>(gp.at(global_parameters::KAdvDataWidth));

  std::vector<std::vector<uint8_t>> rst;
  for (size_t i = 0; i < encryptionFiles.size(); i++) {
    size_t shardSize =
        fbpcf::mpc_std_lib::unified_data_process::data_processor::getShardSize(
            intersectionSize, i, encryptionFiles.size());

    auto [publisherData, advertiserData] = decryptionApp.invokeUdpDecryption(
        encryptionFiles.at(i), expandedKeyFile, parameterFile);

    if constexpr (schedulerId == 0) {
      auto data = publisherData.openToParty(0).getValue();
      advertiserData.openToParty(1);
      auto plaintext = convertToBytes(data, publisherWidth, shardSize);
      rst.insert(rst.end(), plaintext.begin(), plaintext.end());
    } else {
      publisherData.openToParty(0);
      auto data = advertiserData.openToParty(1).getValue();
      auto plaintext = convertToBytes(data, advertiserWidth, shardSize);
      rst.insert(rst.end(), plaintext.begin(), plaintext.end());
    }
  }
  return rst;
}

TEST(UdpEncryptorAppTest, integration_test) {
  auto testdata = generateTestData(100, 87, 42, 31, 19, 3, 7, 2);

  std::vector<std::unique_ptr<fbpcf::engine::communication::
                                  SocketPartyCommunicationAgentFactoryForTests>>
      agentFactories(2);

  fbpcf::engine::communication::SocketPartyCommunicationAgent::TlsInfo tlsInfo;
  tlsInfo.certPath = "";
  tlsInfo.keyPath = "";
  tlsInfo.passphrasePath = "";
  tlsInfo.useTls = false;
  fbpcf::engine::communication::getSocketFactoriesForMultipleParties(
      2, tlsInfo, agentFactories);

  fbpcf::setupRealBackend<0, 1>(*agentFactories[0], *agentFactories[1]);

  auto future1 = std::async(
      test<1>,
      std::move(agentFactories.at(1)),
      testdata.advertiserIndexFiles,
      testdata.advertiserDataFiles,
      testdata.globalParameterFile,
      testdata.advertiserEncryptionFiles,
      testdata.advertiserExpandedKeyFile,
      testdata.intersectionSize);
  auto publisherData = test<0>(
      std::move(agentFactories.at(0)),
      testdata.publisherIndexFiles,
      testdata.publisherDataFiles,
      testdata.globalParameterFile,
      testdata.publisherEncryptionFiles,
      testdata.publisherExpandedKeyFile,
      testdata.intersectionSize);
  auto advertiserData = future1.get();

  ASSERT_EQ(publisherData.size(), testdata.publisherExpectedOutput.size());
  ASSERT_EQ(advertiserData.size(), testdata.advertiserExpectedOutput.size());

  for (size_t i = 0; i < publisherData.size(); i++) {
    fbpcf::testVectorEq(
        publisherData.at(i), testdata.publisherExpectedOutput.at(i));
  }

  for (size_t i = 0; i < advertiserData.size(); i++) {
    fbpcf::testVectorEq(
        advertiserData.at(i), testdata.advertiserExpectedOutput.at(i));
  }
  SCOPE_EXIT {
    for (auto& file : testdata.advertiserDataFiles) {
      std::remove(file.c_str());
    }
    for (auto& file : testdata.advertiserIndexFiles) {
      std::remove(file.c_str());
    }
    for (auto& file : testdata.publisherDataFiles) {
      std::remove(file.c_str());
    }
    for (auto& file : testdata.publisherIndexFiles) {
      std::remove(file.c_str());
    }
    for (auto& file : testdata.publisherEncryptionFiles) {
      std::remove(file.c_str());
    }
    for (auto& file : testdata.advertiserEncryptionFiles) {
      std::remove(file.c_str());
    }
    std::remove(testdata.globalParameterFile.c_str());
    std::remove(testdata.publisherExpandedKeyFile.c_str());
    std::remove(testdata.advertiserExpandedKeyFile.c_str());
  };
}

} // namespace unified_data_process
