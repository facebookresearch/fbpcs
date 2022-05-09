/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fstream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../OutputMetricsData.h"

namespace private_lift {
class LiftCalculator {
 public:
  std::unordered_map<std::string, int32_t> mapColToIndex(
      const std::vector<std::string>& headerPublisher,
      const std::vector<std::string>& headerPartner) const;
  OutputMetricsData compute(
      std::ifstream& inFilePublisher,
      std::ifstream& inFilePartner,
      std::unordered_map<std::string, int32_t>& colNameToIndex,
      int32_t tsOffset,
      bool useAdvancedLift = true) const;

 private:
  // Parse input string with format [111,222,333,...]
  std::vector<uint64_t> parseArray(std::string array) const;
};
} // namespace private_lift
