/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptor.h"

#include "folly/executors/CPUThreadPoolExecutor.h"

namespace unified_data_process {

/**
 * The idea here is to distribute the workload across more threads. This
 * UdpEncryptor object will read in data in the main thread and buffer them.
 * Once there are sufficient buffered data (defined by chunkSize_), the buffered
 * data will be passed to the underlying udp encryption object to process in a
 * background thread.
 */
void UdpEncryptor::processDataInBuffer() {
  if (myDataProcessingFutures_.size() == 0) {
    // this is the first time of executing processingMyData, need to call
    // preparation first
    udpEncryption_->prepareToProcessMyData(bufferForMyData_->at(0).size());
  }
  bufferForMyData_->resize(bufferIndex_);
  bufferIndex_ = 0;
  if (bufferForMyData_->size() > 0) {
    auto [promise, future] = folly::makePromiseContract<folly::Unit>();
    myDataProcessExecutor_->add([this,
                                 data = std::move(bufferForMyData_),
                                 p = std::move(promise)]() mutable {
      udpEncryption_->processMyData(*data);
      p.setValue(folly::Unit());
    });
    myDataProcessingFutures_.push_back(std::move(future));

    bufferForMyData_ =
        std::make_unique<std::vector<std::vector<unsigned char>>>(chunkSize_);
  }
}

// load a line that is to be processed later.
void UdpEncryptor::pushOneLineFromMe(
    std::vector<unsigned char>&& serializedLine) {
  bufferForMyData_->at(bufferIndex_++) = std::move(serializedLine);
  if (bufferIndex_ >= chunkSize_) {
    processDataInBuffer();
  }
}

// load multiple lines into the buffer.
void UdpEncryptor::pushLinesFromMe(
    std::vector<std::vector<unsigned char>>&& serializedLines) {
  size_t inputIndex = 0;

  while (inputIndex < serializedLines.size()) {
    if (chunkSize_ - bufferIndex_ <= serializedLines.size() - inputIndex) {
      std::copy(
          serializedLines.begin() + inputIndex,
          serializedLines.begin() + inputIndex + chunkSize_ - bufferIndex_,
          bufferForMyData_->begin() + bufferIndex_);
      inputIndex += chunkSize_ - bufferIndex_;
      // the buffer is full, the index should be changed to chunkSize_
      bufferIndex_ = chunkSize_;
      processDataInBuffer();
    } else {
      std::copy(
          serializedLines.begin() + inputIndex,
          serializedLines.end(),
          bufferForMyData_->begin() + bufferIndex_);

      bufferIndex_ += serializedLines.size() - inputIndex;
      inputIndex = serializedLines.size();
    }
  }
}

// set the config for peer's data.
void UdpEncryptor::setPeerConfig(
    size_t totalNumberOfPeerRows,
    size_t peerDataWidth,
    const std::vector<uint64_t>& indexes) {
  udpEncryption_->prepareToProcessPeerData(peerDataWidth, indexes);
  size_t numberOfProcessedRow = 0;
  peerDataProcessingFutures_.reserve(totalNumberOfPeerRows / chunkSize_ + 1);

  while (numberOfProcessedRow < totalNumberOfPeerRows) {
    auto numberOfPeerRowsInBatch =
        std::min(chunkSize_, totalNumberOfPeerRows - numberOfProcessedRow);
    auto [promise, future] = folly::makePromiseContract<folly::Unit>();
    peerProcessExecutor_->add(
        [this, numberOfPeerRowsInBatch, p = std::move(promise)]() mutable {
          udpEncryption_->processPeerData(numberOfPeerRowsInBatch);
          p.setValue(folly::Unit());
        });
    peerDataProcessingFutures_.push_back(std::move(future));
    numberOfProcessedRow += chunkSize_;
  }
}

UdpEncryptor::EncryptionResuts UdpEncryptor::getEncryptionResults() {
  folly::collectAll(std::move(peerDataProcessingFutures_)).get();

  auto [ciphertexts, nonces, indexes] = udpEncryption_->getProcessedData();
  return EncryptionResuts{ciphertexts, nonces, indexes};
}

std::vector<__m128i> UdpEncryptor::getExpandedKey() {
  processDataInBuffer();
  folly::collectAll(std::move(myDataProcessingFutures_)).get();

  return udpEncryption_->getExpandedKey();
}

} // namespace unified_data_process
