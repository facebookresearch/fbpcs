/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptor.h"

#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/experimental/coro/BlockingWait.h"
#include "folly/experimental/coro/Collect.h"

namespace unified_data_process {

// load a line that is to be processed later.
void UdpEncryptor::pushOneLineFromMe(
    std::vector<unsigned char>&& /*serializedLine*/) {
  throw std::runtime_error("not implemented");
}

folly::coro::Task<void> UdpEncryptor::processPeerDataCoro(
    size_t numberOfPeerRowsInBatch) {
  udpEncryption_->processPeerData(numberOfPeerRowsInBatch);
  co_return;
}

// set the config for peer's data.
void UdpEncryptor::setPeerConfig(
    size_t totalNumberOfPeerRows,
    size_t peerDataWidth,
    const std::vector<int32_t>& indexes) {
  udpEncryption_->prepareToProcessPeerData(peerDataWidth, indexes);
  size_t numberOfProcessedRow = 0;
  peerDataProcessingTasks_.reserve(totalNumberOfPeerRows / chunkSize_ + 1);

  while (numberOfProcessedRow < totalNumberOfPeerRows) {
    peerDataProcessingTasks_.push_back(
        processPeerDataCoro(
            std::min(chunkSize_, totalNumberOfPeerRows - numberOfProcessedRow))
            .scheduleOn(peerProcessExecutor_.get())
            .start());
    numberOfProcessedRow += chunkSize_;
  }
}

UdpEncryptor::EncryptionResuts UdpEncryptor::getEncryptionResults() {
  folly::coro::blockingWait(
      folly::coro::collectAllRange(std::move(peerDataProcessingTasks_)));

  auto [ciphertexts, nonces, indexes] = udpEncryption_->getProcessedData();
  return EncryptionResuts{ciphertexts, nonces, indexes};
}

std::vector<__m128i> UdpEncryptor::getExpandedKey() const {
  throw std::runtime_error("not implemented");
}

} // namespace unified_data_process
