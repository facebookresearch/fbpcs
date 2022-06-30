/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>
#include <cstdint>
#include <exception>

namespace common {

struct SchedulerStatistics {
  uint64_t nonFreeGates;
  uint64_t freeGates;
  uint64_t sentNetwork;
  uint64_t receivedNetwork;
  folly::dynamic details;

  void add(SchedulerStatistics other) {
    nonFreeGates += other.nonFreeGates;
    freeGates += other.freeGates;
    sentNetwork += other.sentNetwork;
    receivedNetwork += other.receivedNetwork;
    try {
      details = folly::dynamic::merge(details, other.details);
    } catch (std::exception& e) {
      details = std::string("Failed to merge details: ") + e.what();
    }
  }
};

} // namespace common
