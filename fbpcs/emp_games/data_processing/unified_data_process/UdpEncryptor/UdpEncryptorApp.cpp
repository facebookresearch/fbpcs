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
#include <iterator>
#include <string>
#include <thread>
#include <unordered_map>
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/UdpUtil.h"
#include "fbpcs/emp_games/data_processing/global_parameters/GlobalParameters.h"
#include "folly/String.h"
#include "folly/experimental/coro/BlockingWait.h"
#include "folly/experimental/coro/Collect.h"

namespace unified_data_process {

void UdpEncryptorApp::invokeUdpEncryption(
    const std::vector<std::string>& indexFiles,
    const std::vector<std::string>& serializedDataFiles,
    const std::string& globalParameters,
    const std::vector<std::string>& dataFiles,
    const std::string& expandedKeyFile) {
  std::vector<folly::coro::Task<void>> tasks;
  tasks.push_back(processPeerData(indexFiles, globalParameters));
  tasks.push_back(processMyData(serializedDataFiles));
  folly::coro::blockingWait(folly::coro::collectAllRange(std::move(tasks)));

  fbpcf::mpc_std_lib::unified_data_process::data_processor::
      writeExpandedKeyToFile(encryptor_->getExpandedKey(), expandedKeyFile);
  auto results = fbpcf::mpc_std_lib::unified_data_process::data_processor::
      splitEncryptionResults(
          encryptor_->getEncryptionResults(), dataFiles.size());
  for (size_t i = 0; i < dataFiles.size(); i++) {
    fbpcf::mpc_std_lib::unified_data_process::data_processor::
        writeEncryptionResultsToFile(results.at(i), dataFiles.at(i));
  }
}

folly::coro::Task<std::vector<int32_t>> UdpEncryptorApp::readIndexFile(
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
  co_return rst;
}

folly::coro::Task<std::vector<std::vector<unsigned char>>>
UdpEncryptorApp::readDataFile(const std::string& fileName) {
  auto reader = std::make_unique<fbpcf::io::BufferedReader>(
      std::make_unique<fbpcf::io::FileReader>(fileName));

  std::vector<std::vector<unsigned char>> rst;
  while (!reader->eof()) {
    auto line = reader->readLine();
    rst.push_back(std::vector<unsigned char>(line.begin(), line.end()));
  }
  reader->close();
  co_return rst;
}

folly::coro::Task<void> UdpEncryptorApp::processPeerData(
    const std::vector<std::string>& indexFiles,
    const std::string& globalParameterFile) const {
  auto executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(indexFiles.size());

  std::vector<folly::coro::Task<std::vector<int32_t>>> tasks;
  for (auto& file : indexFiles) {
    tasks.push_back(UdpEncryptorApp::readIndexFile(file));
  }
  auto globalParameters = global_parameters::readFromFile(globalParameterFile);
  auto indexInFiles = co_await folly::coro::collectAllRange(std::move(tasks));

  std::vector<int32_t> indexes;
  for (auto& indexInFile : indexInFiles) {
    indexes.insert(
        indexes.end(),
        std::make_move_iterator(indexInFile.begin()),
        std::make_move_iterator(indexInFile.end()));
  }
  auto totalNumberOfPeerRows = boost::get<int32_t>(
      amIPublisher_ ? globalParameters.at(global_parameters::KAdvRowCount)
                    : globalParameters.at(global_parameters::KPubRowCount));
  auto peerDataWidth = boost::get<int32_t>(
      amIPublisher_ ? globalParameters.at(global_parameters::KAdvDataWidth)
                    : globalParameters.at(global_parameters::KPubDataWidth));

  encryptor_->setPeerConfig(totalNumberOfPeerRows, peerDataWidth, indexes);
  co_return;
}

folly::coro::Task<void> UdpEncryptorApp::processMyData(
    const std::vector<std::string>& serializedDataFiles) const {
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      serializedDataFiles.size() + 1);

  std::vector<folly::SemiFuture<std::vector<std::vector<unsigned char>>>> tasks;

  for (size_t i = 1; i < serializedDataFiles.size(); i++) {
    tasks.push_back(readDataFile(serializedDataFiles.at(i))
                        .scheduleOn(executor.get())
                        .start());
  }
  {
    // process the first file in main thread.
    auto reader = std::make_unique<fbpcf::io::BufferedReader>(
        std::make_unique<fbpcf::io::FileReader>(serializedDataFiles.at(0)));
    while (!reader->eof()) {
      auto line = reader->readLine();
      encryptor_->pushOneLineFromMe(
          std::vector<unsigned char>(line.begin(), line.end()));
    }
    reader->close();
  }

  auto data = co_await folly::coro::collectAllRange(std::move(tasks));
  for (auto& datum : data) {
    encryptor_->pushLinesFromMe(std::move(datum));
  }
  co_return;
}

} // namespace unified_data_process
