/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/experimental/coro/Task.h>
#include <memory>
#include <thread>
#include "fbpcf/mpc_std_lib/unified_data_process/data_processor/IUdpEncryption.h"
#include "folly/futures/Future.h"

namespace unified_data_process {

class UdpEncryptor {
  using UdpEncryption =
      fbpcf::mpc_std_lib::unified_data_process::data_processor::IUdpEncryption;

 public:
  using EncryptionResuts = UdpEncryption::EncryptionResults;

  UdpEncryptor(std::unique_ptr<UdpEncryption> udpEncryption, size_t chunkSize)
      : udpEncryption_(std::move(udpEncryption)),
        udpThreadForMySelf_(nullptr),
        chunkSize_(chunkSize),
        bufferIndex_(0),
        bufferForMyData_{
            std::make_unique<std::vector<std::vector<unsigned char>>>(
                chunkSize_)},
        myDataProcessExecutor_(
            std::make_shared<folly::CPUThreadPoolExecutor>(1)),
        peerProcessExecutor_(
            std::make_shared<folly::CPUThreadPoolExecutor>(1)) {}

  // load a line that is to be processed later.
  void pushOneLineFromMe(std::vector<unsigned char>&& serializedLine);

  // load a number of lines that is to be processed later.
  void pushLinesFromMe(
      std::vector<std::vector<unsigned char>>&& serializedLines);

  // set the config for peer's data.
  void setPeerConfig(
      size_t totalNumberOfPeerRows,
      size_t peerDataWidth,
      const std::vector<int32_t>& indexes);

  EncryptionResuts getEncryptionResults();

  std::vector<__m128i> getExpandedKey();

 private:
  folly::coro::Task<void> processPeerDataCoro(size_t numberOfPeerRowsInBatch);

  folly::coro::Task<void> processMyDataCoro(
      std::unique_ptr<std::vector<std::vector<unsigned char>>> data);

  void processDataInBuffer();

  std::unique_ptr<UdpEncryption> udpEncryption_;

  std::unique_ptr<std::thread> udpThreadForMySelf_;
  size_t chunkSize_;

  size_t bufferIndex_;
  std::unique_ptr<std::vector<std::vector<unsigned char>>> bufferForMyData_;

  std::shared_ptr<folly::CPUThreadPoolExecutor> myDataProcessExecutor_;
  std::vector<folly::SemiFuture<folly::Unit>> myDataProcessingTasks_;

  std::shared_ptr<folly::CPUThreadPoolExecutor> peerProcessExecutor_;
  std::vector<folly::SemiFuture<folly::Unit>> peerDataProcessingTasks_;
};

} // namespace unified_data_process
