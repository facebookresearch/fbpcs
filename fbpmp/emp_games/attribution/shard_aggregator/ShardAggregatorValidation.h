/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>
#include <vector>

#include "AggMetrics.h"

namespace measurement::private_attribution {
class InvalidFormatException : public std::runtime_error {
 public:
  explicit InvalidFormatException(const std::string& msg) noexcept
      : std::runtime_error(msg) {}
};

void validateInputData(const std::vector<folly::dynamic>& inputData);
void validateInputDataAggMetrics(
    const std::vector<std::shared_ptr<private_measurement::AggMetrics>>&
        inputData,
    const std::string& metricsFormatType);
} // namespace measurement::private_attribution
