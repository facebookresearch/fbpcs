/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/UdpEncryptor.h"

namespace unified_data_process {

// load a line that is to be processed later.
void UdpEncryptor::pushOneLineFromMe(
    std::vector<unsigned char>&& /*serializedLine*/) {
  throw std::runtime_error("not implemented");
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

std::vector<__m128i> UdpEncryptor::getExpandedKey() const {
  throw std::runtime_error("not implemented");
}

} // namespace unified_data_process
