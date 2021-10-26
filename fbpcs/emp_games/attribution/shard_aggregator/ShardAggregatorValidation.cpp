/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "ShardAggregatorValidation.h"

#include <set>
#include <vector>

#include <fbpcf/common/FunctionalUtil.h>
#include <folly/dynamic.h>
#include <folly/logging/xlog.h>
#include <stdexcept>

#include "AggMetrics.h"

namespace {
using AggMetrics = private_measurement::AggMetrics;
using AggMetricsTag = private_measurement::AggMetricsTag;
using InvalidFormatException =
    measurement::private_attribution::InvalidFormatException;

void checkIsMap(
    const std::shared_ptr<AggMetrics>& metrics,
    const std::string& msg) {
  if (metrics->getTag() != AggMetricsTag::Map) {
    throw InvalidFormatException(msg);
  }
}

void checkIsList(
    const std::shared_ptr<AggMetrics>& metrics,
    const std::string& msg) {
  if (metrics->getTag() != AggMetricsTag::List) {
    throw InvalidFormatException(msg);
  }
}

void validateAdObjectFormatMetrics(
    const std::vector<std::shared_ptr<AggMetrics>>& inputData) {
  for (const auto& ruleToMetrics : inputData) {
    checkIsMap(ruleToMetrics, "Expected rules to be stored in a map");

    if (ruleToMetrics->getAsMap().size() < 1) {
      throw InvalidFormatException("Map contains no rules");
    }

    for (const auto& [rule, metricsMap] : ruleToMetrics->getAsMap()) {
      checkIsMap(
          metricsMap, folly::sformat("Rule [{}] does not map to a map", rule));

      if (metricsMap->getAsMap().size() < 1) {
        throw InvalidFormatException(
            folly::sformat("Rule [{}] does not map to any metrics", rule));
      }

      for (const auto& [aggregationName, aggregationData] :
           metricsMap->getAsMap()) {
        if (aggregationName.compare("measurement")) {
          throw InvalidFormatException(folly::sformat(
              "Unsupported aggregationName [{}] passed to Shard Aggregator",
              aggregationName));
        }

        checkIsMap(aggregationData, "Aggregation data should be a map");
      }
    }
  }
}

void checkMetrics(
    const private_measurement::AggMetrics::MetricsMap& actualMetrics,
    const std::set<std::string>& metricsFound) {
  if (actualMetrics.size() != metricsFound.size()) {
    throw InvalidFormatException(
        "All maps should contain the same lift metrics");
  }
  for (const auto& [metric, value] : actualMetrics) {
    if (metricsFound.find(metric) == metricsFound.end()) {
      throw InvalidFormatException(folly::sformat(
          "Map contains [{}] metric not found in previous map", metric));
    }
  }
}

void validateLiftMetrics(
    const std::vector<std::shared_ptr<AggMetrics>>& inputData) {
  for (const auto& groupedLiftMetrics : inputData) {
    checkIsMap(
        groupedLiftMetrics,
        "Expected grouped lift metrics to be stored in a map");

    if (groupedLiftMetrics->getAsMap().find("metrics") == groupedLiftMetrics->getAsMap().end()) {
      throw InvalidFormatException("Map should contain 'metrics' at a minimum");
    }
    checkIsMap(
        groupedLiftMetrics->getAtKey("metrics"), "metrics should map to a map");
  }
}
} // namespace

namespace measurement::private_attribution {
void validateInputDataAggMetrics(
    const std::vector<std::shared_ptr<AggMetrics>>& inputData,
    const std::string& metricsFormatType) {
  if (inputData.size() < 1) {
    throw InvalidFormatException("Input is empty");
  }

  if (metricsFormatType == "ad_object") {
    validateAdObjectFormatMetrics(inputData);

  } else if (metricsFormatType == "lift") {
    validateLiftMetrics(inputData);

  } else {
    throw std::runtime_error(folly::sformat(
        "Unsupported format type {} passed to aggregator", metricsFormatType));
  }
}
} // namespace measurement::private_attribution
