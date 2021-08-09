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
  // ensure all metric maps have same metrics
  std::set<std::string> metricsFound;

  for (const auto& groupedLiftMetrics : inputData) {
    checkIsMap(
        groupedLiftMetrics,
        "Expected grouped lift metrics to be stored in a map");

    if (groupedLiftMetrics->getAsMap().size() != 2 ||
        groupedLiftMetrics->getAsMap().find("cohortMetrics") ==
            groupedLiftMetrics->getAsMap().end() ||
        groupedLiftMetrics->getAsMap().find("metrics") ==
            groupedLiftMetrics->getAsMap().end()) {
      throw InvalidFormatException(
          "Map should contain cohortMetrics and metrics");
    }
    checkIsList(
        groupedLiftMetrics->getAtKey("cohortMetrics"),
        "cohortMetrics should map to a list");
    checkIsMap(
        groupedLiftMetrics->getAtKey("metrics"), "metrics should map to a map");

    // check cohort metrics
    auto cohortMetrics =
        groupedLiftMetrics->getAtKey("cohortMetrics")->getAsList();
    for (std::size_t i = 0; i < cohortMetrics.size(); ++i) {
      checkIsMap(cohortMetrics.at(i), "Cohort {} should be a map");
      auto metrics = cohortMetrics.at(i)->getAsMap();
      if (i == 0) {
        for (const auto& [metric, value] : metrics) {
          // build the expected metrics
          metricsFound.emplace(metric);
        }
      } else {
        checkMetrics(metrics, metricsFound);
      }
    }

    // check metrics
    auto metrics = groupedLiftMetrics->getAtKey("metrics")->getAsMap();
    if (metricsFound.size() != 0) {
      checkMetrics(metrics, metricsFound);
    }
  }
}
} // namespace

namespace measurement::private_attribution {
void validateInputData(const std::vector<folly::dynamic>& inputData) {
  if (inputData.size() < 1) {
    throw InvalidFormatException("Input is empty");
  }

  for (const auto& ruleToMetrics : inputData) {
    if (ruleToMetrics.size() < 1) {
      throw InvalidFormatException("Map contains no rules");
    }

    for (const auto& [rule, metricsMap] : ruleToMetrics.items()) {
      if (metricsMap.size() < 1) {
        throw InvalidFormatException(
            folly::sformat("Rule [{}] does not map to any metrics", rule));
      }

      for (const auto& [aggregationName, aggregationData] :
           metricsMap.items()) {
        if (aggregationName.asString().compare("measurement")) {
          throw InvalidFormatException(folly::sformat(
              "Unsupported aggregationName [{}] passed to Shard Aggregator",
              aggregationName));
        }
      }
    }
  }
}

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
