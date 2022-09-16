/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include "fbpcs/emp_games/lift/pcf2_calculator/LiftGameProcessedData.h"

namespace private_lift {

template <int schedulerId>
void LiftGameProcessedData<schedulerId>::writeToCSV(
    const std::string& globalParamsOutputPath,
    const std::string& secretSharesOutputPath) const {
  std::vector<std::vector<std::string>> globalParams = {
      {std::to_string(numPartnerCohorts),
       std::to_string(numPublisherBreakdowns),
       std::to_string(numGroups),
       std::to_string(numTestGroups),
       std::to_string(valueBits),
       std::to_string(valueSquaredBits)}};

  private_measurement::csv::writeCsv(
      globalParamsOutputPath, GLOBAL_PARAMS_HEADER, globalParams);
}

template <int schedulerId>
LiftGameProcessedData<schedulerId>
LiftGameProcessedData<schedulerId>::readFromCSV(
    const std::string& globalParamsInputPath,
    const std::string& secretSharesInputPath) {
  LiftGameProcessedData<schedulerId> result;
  result.numRows = 0;

  private_measurement::csv::readCsv(
      globalParamsInputPath,
      [&result](
          const std::vector<std::string>& header,
          const std::vector<std::string>& parts) {
        for (size_t i = 0; i < header.size(); i++) {
          auto column = header[i];
          auto value = parts[i];
          if (column == "numPartnerCohorts") {
            result.numPartnerCohorts = std::stoul(value);
          } else if (column == "numPublisherBreakdowns") {
            result.numPublisherBreakdowns = std::stoul(value);
          } else if (column == "numGroups") {
            result.numGroups = std::stoul(value);
          } else if (column == "numTestGroups") {
            result.numTestGroups = std::stoul(value);
          } else if (column == "valueBits") {
            result.valueBits = std::stoul(value);
          } else if (column == "valueSquaredBits") {
            result.valueSquaredBits = std::stoul(value);
          } else {
            LOG(WARNING) << "Warning: Unknown column in csv: " << column;
          }
        }
      });

  return result;
}
} // namespace private_lift
