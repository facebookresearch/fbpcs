/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptorApp.h"
#include <boost/serialization/vector.hpp>
#include <fbpcf/io/api/BufferedReader.h>
#include <fbpcf/io/api/BufferedWriter.h>
#include <fbpcf/io/api/FileReader.h>
#include <fbpcf/io/api/FileWriter.h>
#include <cstdint>
#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/UdpUtil.h"
#include "fbpcs/emp_games/data_processing/global_parameters/GlobalParameters.h"
#include "folly/String.h"

namespace unified_data_process {

void UdpEncryptorApp::invokeUdpEncryption(
    const std::vector<std::string>& indexFiles,
    const std::vector<std::string>& serializedDataFiles,
    const std::string& globalParameters,
    const std::string& dataFile,
    const std::string& expandedKeyFile) {
  auto t = std::thread(
      [this](
          const std::vector<std::string>& indexFiles,
          const std::string& globalParameters) {
        processPeerData(indexFiles, globalParameters);
      },
      indexFiles,
      globalParameters);
  std::vector<std::future<std::vector<std::vector<unsigned char>>>> futures;

  for (size_t i = 1; i < serializedDataFiles.size(); i++) {
    futures.push_back(
        std::async(UdpEncryptorApp::readDataFile, serializedDataFiles.at(i)));
  }
  {
    // process the first file in main thread.
    auto reader = std::make_unique<fbpcf::io::BufferedReader>(
        std::make_unique<fbpcf::io::FileReader>(serializedDataFiles.at(0)));
    reader->readLine(); // header, useless
    while (!reader->eof()) {
      auto line = reader->readLine();

      encryptor_->pushOneLineFromMe(
          std::vector<unsigned char>(line.begin(), line.end()));
    }
    reader->close();
  }
  for (auto& future : futures) {
    encryptor_->pushLinesFromMe(future.get());
  }
  t.join();
  fbpcf::mpc_std_lib::unified_data_process::data_processor::
      writeEncryptionResultsToFile(
          encryptor_->getEncryptionResults(), dataFile);
  fbpcf::mpc_std_lib::unified_data_process::data_processor::
      writeExpandedKeyToFile(encryptor_->getExpandedKey(), expandedKeyFile);
}

std::vector<int32_t> UdpEncryptorApp::readIndexFile(
    const std::string& fileName) {
  auto reader = std::make_unique<fbpcf::io::BufferedReader>(
      std::make_unique<fbpcf::io::FileReader>(fileName));
  reader->readLine(); // header, useless

  std::vector<int32_t> rst;
  while (!reader->eof()) {
    std::vector<std::string> data;
    auto line = reader->readLine();
    folly::split(",", std::move(line), data);
    rst.push_back(stoi(data.at(1)));
  }
  reader->close();
  return rst;
}

std::vector<std::vector<unsigned char>> UdpEncryptorApp::readDataFile(
    const std::string& fileName) {
  auto reader = std::make_unique<fbpcf::io::BufferedReader>(
      std::make_unique<fbpcf::io::FileReader>(fileName));
  reader->readLine(); // header, useless

  std::vector<std::vector<unsigned char>> rst;
  while (!reader->eof()) {
    auto line = reader->readLine();
    rst.push_back(std::vector<unsigned char>(line.begin(), line.end()));
  }
  reader->close();
  return rst;
}

void UdpEncryptorApp::processPeerData(
    const std::vector<std::string>& indexFiles,
    const std::string& globalParameterFile) const {
  std::vector<std::future<std::vector<int32_t>>> futures;
  for (auto& file : indexFiles) {
    futures.push_back(std::async(UdpEncryptorApp::readIndexFile, file));
  }

  auto globalParameters = global_parameters::readFromFile(globalParameterFile);

  std::vector<int32_t> indexes;
  for (auto& future : futures) {
    auto indexInFile = future.get();
    indexes.insert(indexes.end(), indexInFile.begin(), indexInFile.end());
  }
  auto totalNumberOfPeerRows = boost::get<int32_t>(
      amIPublisher_ ? globalParameters.at(global_parameters::KAdvRowCount)
                    : globalParameters.at(global_parameters::KPubRowCount));
  auto peerDataWidth = boost::get<int32_t>(
      amIPublisher_ ? globalParameters.at(global_parameters::KAdvDataWidth)
                    : globalParameters.at(global_parameters::KPubDataWidth));

  encryptor_->setPeerConfig(totalNumberOfPeerRows, peerDataWidth, indexes);
}

} // namespace unified_data_process
