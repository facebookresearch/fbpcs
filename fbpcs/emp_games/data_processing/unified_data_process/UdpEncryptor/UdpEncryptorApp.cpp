/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptorApp.h"

#include <fbpcf/io/api/BufferedReader.h>
#include <fbpcf/io/api/BufferedWriter.h>
#include <fbpcf/io/api/FileReader.h>
#include <fbpcf/io/api/FileWriter.h>
#include <cstdint>
#include <iterator>
#include <string>
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/UdpUtil.h"
#include "fbpcs/emp_games/data_processing/global_parameters/GlobalParameters.h"
#include "folly/String.h"

namespace unified_data_process {

void UdpEncryptorApp::invokeUdpEncryption(
    const std::vector<std::string>& indexFiles,
    const std::vector<std::string>& serializedDataFiles,
    const std::string& globalParameters,
    const std::vector<std::string>& dataFiles,
    const std::string& expandedKeyFile) {
  std::vector<folly::SemiFuture<folly::Unit>> futures;
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(2);
  {
    auto [promise, future] = folly::makePromiseContract<folly::Unit>();
    executor->add(
        [this, &serializedDataFiles, p = std::move(promise)]() mutable {
          processMyData(serializedDataFiles);
          p.setValue(folly::Unit());
        });
    futures.push_back(std::move(future));
  }
  {
    auto [promise, future] = folly::makePromiseContract<folly::Unit>();
    executor->add([this,
                   &indexFiles,
                   &globalParameters,
                   p = std::move(promise)]() mutable {
      processPeerData(indexFiles, globalParameters);
      p.setValue(folly::Unit());
    });
    futures.push_back(std::move(future));
  }

  folly::collectAll(std::move(futures)).get();

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
  auto executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(indexFiles.size());

  std::vector<folly::SemiFuture<std::vector<int32_t>>> futures;
  for (auto& file : indexFiles) {
    auto [promise, future] = folly::makePromiseContract<std::vector<int32_t>>();
    executor->add([&file, p = std::move(promise)]() mutable {
      p.setValue(UdpEncryptorApp::readIndexFile(file));
    });
    futures.push_back(std::move(future));
  }
  auto globalParameters = global_parameters::readFromFile(globalParameterFile);
  auto indexInFiles = folly::collectAll(std::move(futures)).get();

  std::vector<int32_t> indexes;
  for (auto& indexInFileFuture : indexInFiles) {
    indexInFileFuture.throwUnlessValue();
    auto& indexInFile = indexInFileFuture.value();
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
  return;
}

void UdpEncryptorApp::processMyData(
    const std::vector<std::string>& serializedDataFiles) const {
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      serializedDataFiles.size() + 1);

  std::vector<folly::SemiFuture<std::vector<std::vector<unsigned char>>>>
      futures;

  for (size_t i = 1; i < serializedDataFiles.size(); i++) {
    auto [promise, future] =
        folly::makePromiseContract<std::vector<std::vector<unsigned char>>>();
    executor->add(
        [file = serializedDataFiles.at(i), p = std::move(promise)]() mutable {
          p.setValue(readDataFile(file));
        });
    futures.push_back(std::move(future));
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

  auto data = folly::collectAll(std::move(futures)).get();
  for (auto& datum : data) {
    datum.throwUnlessValue();
    encryptor_->pushLinesFromMe(std::move(datum.value()));
  }
  return;
}

} // namespace unified_data_process
