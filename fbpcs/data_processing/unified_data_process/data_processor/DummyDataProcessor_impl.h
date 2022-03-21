/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace unified_data_process::data_processor::insecure {

template <int schedulerId>
typename IDataProcessor<schedulerId>::SecString
DummyDataProcessor<schedulerId>::processMyData(
    const std::vector<std::vector<unsigned char>>& plaintextData,
    size_t outputSize) {
  if (plaintextData.size() == 0) {
    throw std::runtime_error("payload can't be empty!");
  }
  if (outputSize == 0) {
    throw std::runtime_error("output can't be empty!");
  }
  for (auto& item : plaintextData) {
    agent_->send(item);
  }
  std::vector<std::vector<bool>> dummyShare(
      plaintextData.at(0).size() * 8, std::vector<bool>(outputSize));
  return
      typename IDataProcessor<schedulerId>::SecString(dummyShare, partnerId_);
}

template <int schedulerId>
typename IDataProcessor<schedulerId>::SecString
DummyDataProcessor<schedulerId>::processPeersData(
    size_t dataSize,
    const std::vector<int64_t>& indexes,
    size_t dataWidth) {
  std::vector<std::vector<unsigned char>> plaintext;
  for (size_t i = 0; i < dataSize; i++) {
    plaintext.push_back(agent_->receive(dataWidth));
  }
  std::vector<std::vector<bool>> myShare(
      dataWidth * 8, std::vector<bool>(indexes.size()));
  for (size_t i = 0; i < dataWidth; i++) {
    for (uint8_t j = 0; j < 8; j++) {
      for (size_t k = 0; k < indexes.size(); k++) {
        myShare[i * 8 + j][k] = (plaintext.at(indexes.at(k)).at(i) >> j) & 1;
      }
    }
  }
  return typename IDataProcessor<schedulerId>::SecString(myShare, myId_);
}

} // namespace unified_data_process::data_processor::insecure
