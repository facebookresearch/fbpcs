/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace common {

struct SchedulerStatistics {
  uint64_t nonFreeGates;
  uint64_t freeGates;
  uint64_t sentNetwork;
  uint64_t receivedNetwork;

  void add(SchedulerStatistics other) {
    nonFreeGates += other.nonFreeGates;
    freeGates += other.freeGates;
    sentNetwork += other.sentNetwork;
    receivedNetwork += other.receivedNetwork;
  }
};

} // namespace common
