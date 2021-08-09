/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <emp-tool/emp-tool.h>

namespace private_lift {
/*
 * Simple struct representing the metrics in an encrypted Lift computation.
 * These values are not readable until they have been revealed.
 */
struct EncryptedLiftMetrics {
  emp::Integer testPopulation;
  emp::Integer controlPopulation;
  emp::Integer testConversions;
  emp::Integer controlConversions;
  emp::Integer testConverters;
  emp::Integer controlConverters;
  emp::Integer testValue;
  emp::Integer controlValue;
  emp::Integer testValueSquared;
  emp::Integer controlValueSquared;
  emp::Integer testNumConvSquared;
  emp::Integer controlNumConvSquared;
  emp::Integer testMatchCount;
  emp::Integer controlMatchCount;
  emp::Integer testImpressions;
  emp::Integer controlImpressions;
  emp::Integer testClicks;
  emp::Integer controlClicks;
  emp::Integer testSpend;
  emp::Integer controlSpend;
  emp::Integer testReach;
  emp::Integer controlReach;
  emp::Integer testClickers;
  emp::Integer controlClickers;
  emp::Integer reachedConversions;
  emp::Integer reachedValue;

  EncryptedLiftMetrics operator+(
      const EncryptedLiftMetrics& other) const noexcept;
  EncryptedLiftMetrics operator^(
      const EncryptedLiftMetrics& other) const noexcept;
};
} // namespace private_lift
