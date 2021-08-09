/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "EncryptedLiftMetrics.h"

namespace private_lift {
struct GroupedEncryptedLiftMetrics {
  EncryptedLiftMetrics metrics;
  std::vector<EncryptedLiftMetrics> cohortMetrics;

  GroupedEncryptedLiftMetrics operator+(const GroupedEncryptedLiftMetrics& other) const noexcept;
  GroupedEncryptedLiftMetrics operator^(const GroupedEncryptedLiftMetrics& other) const noexcept;
};

} // namespace private_lift
