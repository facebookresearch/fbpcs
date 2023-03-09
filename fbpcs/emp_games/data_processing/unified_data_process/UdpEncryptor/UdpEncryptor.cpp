/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptor.h"
#include <thread>

namespace unified_data_process {

/**
 * The idea here is to distribute the workload across more threads. This
 * UdpEncryptor object will read in data in the main thread and buffer them.
 * Once there are sufficient buffered data (defined by chunkSize_), the buffered
 * data will be passed to the underlying udp encryption object to process in a
 * background thread.
 */
void UdpEncryptor::processDataInBuffer() {
  if (udpThreadForMySelf_ != nullptr) {
    // this is not the first time of executing processingMyData
    udpThreadForMySelf_->join();
  } else {
    // this is the first time of executing processingMyData, need to call
    // preparation first
    udpEncryption_->prepareToProcessMyData(
        bufferForMyDataInLoading_->at(0).size());
  }
  if (bufferIndex_ < chunkSize_) {
    bufferForMyDataInLoading_->resize(bufferIndex_);
  }
  std::swap(bufferForMyDataInLoading_, bufferForMyDataInProcessing_);
  bufferIndex_ = 0;
  udpThreadForMySelf_ = std::make_unique<std::thread>([this]() {
    if (bufferForMyDataInProcessing_->size() == 0) {
      return;
    }
    udpEncryption_->processMyData(*bufferForMyDataInProcessing_);
  });
}

// load a line that is to be processed later.
void UdpEncryptor::pushOneLineFromMe(
    std::vector<unsigned char>&& serializedLine) {
  bufferForMyDataInLoading_->at(bufferIndex_++) = std::move(serializedLine);
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
          bufferForMyDataInLoading_->begin() + bufferIndex_);
      inputIndex += chunkSize_ - bufferIndex_;
      // the buffer is full, the index should be changed to chunkSize_
      bufferIndex_ = chunkSize_;
      processDataInBuffer();
    } else {
      std::copy(
          serializedLines.begin() + inputIndex,
          serializedLines.end(),
          bufferForMyDataInLoading_->begin() + bufferIndex_);

      bufferIndex_ += serializedLines.size() - inputIndex;
      inputIndex = serializedLines.size();
    }
  }
}

// set the config for peer's data.
void UdpEncryptor::setPeerConfig(
    size_t totalNumberOfPeerRows,
    size_t peerDataWidth,
    const std::vector<int32_t>& indexes) {
  udpEncryption_->prepareToProcessPeerData(peerDataWidth, indexes);
  auto loop = [this, totalNumberOfPeerRows]() {
    size_t numberOfProcessedRow = 0;
    while (numberOfProcessedRow < totalNumberOfPeerRows) {
      udpEncryption_->processPeerData(
          std::min(chunkSize_, totalNumberOfPeerRows - numberOfProcessedRow));
      numberOfProcessedRow += chunkSize_;
    }
  };
  udpThreadForPeer_ = std::make_unique<std::thread>(loop);
}

UdpEncryptor::EncryptionResuts UdpEncryptor::getEncryptionResults() const {
  if (udpThreadForPeer_ == nullptr) {
    throw std::runtime_error("No thread to join for peer!");
  }
  udpThreadForPeer_->join();

  auto [ciphertexts, nonces, indexes] = udpEncryption_->getProcessedData();
  return EncryptionResuts{ciphertexts, nonces, indexes};
}

std::vector<__m128i> UdpEncryptor::getExpandedKey() {
  processDataInBuffer();
  if (udpThreadForMySelf_ == nullptr) {
    throw std::runtime_error("No thread to join for peer!");
  }
  udpThreadForMySelf_->join();
  return udpEncryption_->getExpandedKey();
}

} // namespace unified_data_process
