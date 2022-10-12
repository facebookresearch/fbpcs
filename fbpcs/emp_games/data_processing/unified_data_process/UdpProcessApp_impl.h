/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/data_processing/unified_data_process/UdpProcessApp.h"

#include <fbpcf/scheduler/LazySchedulerFactory.h>
#include <fbpcf/scheduler/NetworkPlaintextSchedulerFactory.h>
#include <cstddef>

namespace unified_data_process {

template <int schedulerId>
std::tuple<std::vector<std::vector<bool>>, std::vector<std::vector<bool>>>
UdpProcessApp<schedulerId>::run() {
  auto scheduler = createScheduler();

  XLOG(INFO) << "Start generating random data...";
  auto testData = dataGeneration();
  XLOG(INFO) << "Finsihed generating random data...";
  auto& unionMap = std::get<0>(testData);
  auto& metaData = std::get<1>(testData);
  auto udpProcessGame = udpGameFactory_->create(std::move(scheduler));

  XLOGF(
      INFO, "Start to run Adapter with a unionMap of size {}", unionMap.size());
  auto indexes = udpProcessGame->playAdapter(unionMap);

  XLOGF(
      INFO,
      "Start to run DataProcessor with a metaData of size {} and intersection size of {}",
      metaData.size(),
      indexes.size());
  auto shares = udpProcessGame->playDataProcessor(
      metaData, indexes, metaData.size(), sizeOfRow_);

  auto publisherShares = std::get<0>(shares);
  auto partnerShares = std::get<1>(shares);

  XLOGF(
      INFO,
      "Finished UDP library with publisher shares (batch size {} and bitlength {}) and partner shares (batch size {} and bitlength {})",
      publisherShares.at(0).size(),
      publisherShares.size(),
      partnerShares.at(0).size(),
      partnerShares.size());

  auto gateStatistics =
      fbpcf::scheduler::SchedulerKeeper<schedulerId>::getGateStatistics();

  XLOGF(
      INFO,
      "Non-free gate count = {}, Free gate count = {}",
      gateStatistics.first,
      gateStatistics.second);

  auto trafficStatistics =
      fbpcf::scheduler::SchedulerKeeper<schedulerId>::getTrafficStatistics();
  XLOGF(
      INFO,
      "Sent network traffic = {}, Received network traffic = {}",
      trafficStatistics.first,
      trafficStatistics.second);

  schedulerStatistics_.nonFreeGates = gateStatistics.first;
  schedulerStatistics_.freeGates = gateStatistics.second;
  schedulerStatistics_.sentNetwork = trafficStatistics.first;
  schedulerStatistics_.receivedNetwork = trafficStatistics.second;
  schedulerStatistics_.details = metricCollector_->collectMetrics();

  return {publisherShares, partnerShares};
}

template <int schedulerId>
std::unique_ptr<fbpcf::scheduler::IScheduler>
UdpProcessApp<schedulerId>::createScheduler() {
  return useXorEncryption_
      ? fbpcf::scheduler::getLazySchedulerFactoryWithRealEngine(
            party_, *communicationAgentFactory_, metricCollector_)
            ->create()
      : fbpcf::scheduler::NetworkPlaintextSchedulerFactory<false>(
            party_, *communicationAgentFactory_, metricCollector_)
            .create();
}

template <int schedulerId>
std::tuple<std::vector<int64_t>, std::vector<std::vector<unsigned char>>>
UdpProcessApp<schedulerId>::dataGeneration() {
  std::vector<int64_t> unionMap(numberOfRows_, -1);
  std::vector<std::vector<unsigned char>> metaData(
      (numberOfRows_ - numberOfIntersection_) / 2 + numberOfIntersection_,
      std::vector<unsigned char>(sizeOfRow_));

  for (size_t i = 0; i < numberOfIntersection_; ++i) {
    unionMap[i] = i;
    for (size_t j = 0; j < sizeOfRow_; ++j) {
      metaData[i][j] = i % 256;
    }
  }
  for (size_t i = numberOfIntersection_; i < unionMap.size(); ++i) {
    // If an entry is non-match, it means that only one party has a match.
    // We use -party_ to represents a non-match entry so that publisher has 0
    // (match) but partner has -1 (non-match)
    unionMap[i] = -party_;
  }
  std::random_device rd;
  std::mt19937_64 e(rd());
  std::uniform_int_distribution<uint8_t> dist(0, 255);
  for (size_t i = numberOfIntersection_; i < metaData.size(); ++i) {
    for (size_t j = 0; j < sizeOfRow_; ++j) {
      metaData[i][j] = dist(e);
    }
  }
  return {unionMap, metaData};
}

} // namespace unified_data_process
