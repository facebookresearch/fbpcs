/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessGame.h"

namespace unified_data_process {

template <int schedulerId>
std::vector<int32_t> UdpProcessGame<schedulerId>::playAdapter(
    const std::vector<int32_t>& unionMap) {
  auto adapter = adapterFactory_->create();
  return adapter->adapt(unionMap);
}

template <int schedulerId>
std::tuple<std::vector<std::vector<bool>>, std::vector<std::vector<bool>>>
UdpProcessGame<schedulerId>::playDataProcessor(
    const std::vector<std::vector<unsigned char>>& metaData,
    const std::vector<int32_t>& indexes,
    size_t peersDataSize,
    size_t peersDataWidth) {
  size_t intersectionSize = indexes.size();
  auto dataProcessor = dataProcessorFactory_->create();
  typename UdpProcessGame<schedulerId>::SecString publisherShares;
  typename UdpProcessGame<schedulerId>::SecString advertiserShares;

  std::vector<uint64_t> uint64Index(indexes.size());
  for (size_t i = 0; i < indexes.size(); i++) {
    uint64Index.at(i) = indexes.at(i);
  }

  if (myId_ == common::PUBLISHER) {
    XLOG(INFO) << "Start to process my data...";
    publisherShares = dataProcessor->processMyData(metaData, intersectionSize);
    XLOG(INFO) << "Start to process peer's data...";
    advertiserShares = dataProcessor->processPeersData(
        peersDataSize, uint64Index, peersDataWidth);
  } else {
    XLOG(INFO) << "Start to process peer's data...";
    publisherShares = dataProcessor->processPeersData(
        peersDataSize, uint64Index, peersDataWidth);
    XLOG(INFO) << "Start to process my data...";
    advertiserShares = dataProcessor->processMyData(metaData, intersectionSize);
  }
  std::vector<std::vector<bool>> publisherRawShare(
      publisherShares.size(),
      std::vector<bool>(publisherShares.getBatchSize()));
  std::vector<std::vector<bool>> advertiserRawShare(
      advertiserShares.size(),
      std::vector<bool>(advertiserShares.getBatchSize()));

  auto extractPublisherString = publisherShares.extractStringShare();
  auto extractAdvertiserString = advertiserShares.extractStringShare();

  for (size_t i = 0; i < extractPublisherString.size(); ++i) {
    publisherRawShare[i] = extractPublisherString[i].getValue();
  }
  for (size_t i = 0; i < extractAdvertiserString.size(); ++i) {
    advertiserRawShare[i] = extractAdvertiserString[i].getValue();
  }

  return {publisherRawShare, advertiserRawShare};
}
} // namespace unified_data_process
