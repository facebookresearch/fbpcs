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
  costEst_->addCheckPoint("computation preparation");
  XLOGF(
      INFO, "Start to run Adapter with a unionMap of size {}", unionMap.size());
  auto indexes = udpProcessGame->playAdapter(unionMap);
  costEst_->addCheckPoint("Adapter done");
  XLOGF(
      INFO,
      "Start to run DataProcessor with a metaData of size {} and intersection size of {}",
      metaData.size(),
      indexes.size());
  auto shares = udpProcessGame->playDataProcessor(
      metaData, indexes, metaData.size(), sizeOfRow_);
  costEst_->addCheckPoint("DataProcessor done");
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
std::tuple<std::vector<int32_t>, std::vector<std::vector<unsigned char>>>
UdpProcessApp<schedulerId>::dataGeneration() {
  std::vector<int32_t> unionMap(numberOfRows_, -1);
  uint32_t unmatchedCount = numberOfRows_ - numberOfIntersection_;
  uint32_t p0UmatchedCount = unmatchedCount / 2 + unmatchedCount % 2;
  uint32_t p1UmatchedCount = unmatchedCount / 2;

  std::vector<std::vector<unsigned char>> metaData(
      party_ == 0 ? p0UmatchedCount : p1UmatchedCount,
      std::vector<unsigned char>(sizeOfRow_));

  for (size_t i = 0; i < numberOfIntersection_; ++i) {
    unionMap[i] = i;
    for (size_t j = 0; j < sizeOfRow_; ++j) {
      metaData[i][j] = i % 256;
    }
  }

  for (size_t i = 0; i < unmatchedCount; ++i) {
    // assign the rest of the indexes in alternating order so there are no more
    // matches
    if (i % 2 == party_) {
      unionMap[numberOfIntersection_ + i] = numberOfIntersection_ + i / 2;
    } else {
      unionMap[numberOfIntersection_ + i] = -1;
    }
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
