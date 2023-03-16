/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptor.h"

namespace unified_data_process {

class UdpEncryptorApp {
 public:
  ~UdpEncryptorApp() {}

  UdpEncryptorApp(std::unique_ptr<UdpEncryptor> encryptor, bool amIPublisher)
      : encryptor_(std::move(encryptor)), amIPublisher_(amIPublisher) {}

  void invokeUdpEncryption(
      const std::vector<std::string>& indexFiles,
      const std::vector<std::string>& serializedDataFiles,
      const std::string& globalParameters,
      const std::vector<std::string>& dataFiles,
      const std::string& expandedKeyFile);

 private:
  static folly::coro::Task<std::vector<int32_t>> readIndexFile(
      const std::string& fileName);
  static folly::coro::Task<std::vector<std::vector<unsigned char>>>
  readDataFile(const std::string& fileName);

  folly::coro::Task<void> processPeerData(
      const std::vector<std::string>& indexFiles,
      const std::string& globalParameterFile) const;

  folly::coro::Task<void> processMyData(
      const std::vector<std::string>& serializedDataFiles) const;

  std::unique_ptr<UdpEncryptor> encryptor_;
  bool amIPublisher_;
};

} // namespace unified_data_process
