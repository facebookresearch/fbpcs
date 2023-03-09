/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <set>
#include "folly/String.h"

namespace private_measurement {
bool inline isFeatureFlagEnabled(
    const std::string& featureFlags,
    const std::string& featureFlag) {
  std::set<std::string> enabledFeatureFlags;
  folly::splitTo<std::string>(
      ',',
      featureFlags,
      std::inserter(enabledFeatureFlags, enabledFeatureFlags.begin()),
      true);
  return enabledFeatureFlags.contains(featureFlag);
}
} // namespace private_measurement
