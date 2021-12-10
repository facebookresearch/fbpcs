/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <emp-sh2pc/emp-sh2pc.h>
#include <folly/dynamic.h>
#include <folly/json.h>

#include <fbpcf/mpc/EmpGame.h>
#include "Aggregator.h"
#include "AttributionRule.h"
#include "Constants.h"
#include "Conversion.h"
#include "Touchpoint.h"
#include "fbpcs/emp_games/common/PrivateData.h"

namespace measurement::private_attribution {

/*
 * This class represents input data for a Private Attribution computation.
 * It processes an input csv and generates the std::vectors for each column
 */
class AttributionInputMetrics {
 public:
  // Constructor -- input is a path to a CSV
  explicit AttributionInputMetrics(
      int myRole,
      std::string attributionRules,
      std::string aggregationFormatNamesStr,
      std::filesystem::path filepath);

  const std::vector<int64_t>& getIds() const {
    return ids_;
  }

  const std::vector<AttributionRule>& getAttributionRules() const {
    return attributionRules_;
  }

  const std::vector<AggregationFormat>& getAggregationFormats() const {
    return aggregationFormats_;
  }

  const std::vector<std::vector<Conversion>>& getConversionArrays() const {
    return convArrays_;
  }

  const std::vector<std::vector<Touchpoint>>& getTouchpointArrays() const {
    return tpArrays_;
  }

 private:
  std::vector<int64_t> ids_;
  std::vector<AttributionRule> attributionRules_;
  std::vector<AggregationFormat> aggregationFormats_;
  std::vector<std::vector<Touchpoint>> tpArrays_;
  std::vector<std::vector<Conversion>> convArrays_;
};

struct AttributionMetrics {
  std::unordered_map<std::string, Aggregation> formatToAggregation;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();
    for (auto kv : formatToAggregation) {
      auto aggregationName = kv.first;
      auto aggregationMetrics = kv.second;
      res.insert(aggregationName, aggregationMetrics);
    }

    return res;
  }

  static AttributionMetrics fromDynamic(const folly::dynamic& obj) {
    std::unordered_map<std::string, Aggregation> formatToAggregation;
    for (auto& pair : obj.items()) {
      auto aggregationName = pair.first.asString();
      auto aggregationMetrics = pair.second;
      std::pair<std::string, Aggregation> t{
          aggregationName, aggregationMetrics};
      formatToAggregation.insert(t);
    }

    AttributionMetrics metrics{};
    metrics.formatToAggregation = formatToAggregation;

    return metrics;
  }

  std::string toJson() {
    auto obj = toDynamic();
    return folly::toJson(obj);
  }

  static AttributionMetrics fromJson(const std::string& str) {
    auto obj = folly::parseJson(str);
    return fromDynamic(obj);
  }
};

class PrivateAttributionMetrics {
 public:
  PrivateAttributionMetrics(
      AttributionRule attributionRule,
      std::vector<AggregationFormat> aggregationFormats_,
      const AggregationContext& ctx,
      const fbpcf::Visibility& outputVisibility)
      : _attributionRule(attributionRule) {
    for (auto aggregationFormat : aggregationFormats_) {
      formatToAggregator[aggregationFormat.name] =
          aggregationFormat.newAggregator(
              attributionRule, ctx, outputVisibility);
    }
  }

  void addAttribution(const PrivateAttribution& attribution) {
    for (auto& kv : formatToAggregator) {
      kv.second->addAttribution(attribution);
    }
  }

  AttributionMetrics reveal() {
    AttributionMetrics out;

    for (const auto& [format, aggregator] : formatToAggregator) {
      out.formatToAggregation[format] = aggregator->reveal();
    }

    return out;
  }

 private:
  AttributionRule _attributionRule;
  std::unordered_map<std::string, std::unique_ptr<Aggregator>>
      formatToAggregator;
};

struct AttributionOutputMetrics {
  std::unordered_map<std::string, AttributionMetrics> ruleToMetrics;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();
    for (auto& kv : ruleToMetrics) {
      auto ruleName = kv.first;
      auto metrics = kv.second;
      res.insert(ruleName, metrics.toDynamic());
    }

    return res;
  }

  static AttributionOutputMetrics fromDynamic(const folly::dynamic& obj) {
    AttributionOutputMetrics metrics;
    std::unordered_map<std::string, AttributionMetrics> ruleToMetrics;

    for (auto& pair : obj.items()) {
      std::string ruleName = pair.first.asString();
      auto attributionMetrics = AttributionMetrics::fromDynamic(pair.second);
      std::pair<std::string, AttributionMetrics> t{
          ruleName, attributionMetrics};
      ruleToMetrics.insert(t);
    }

    metrics.ruleToMetrics = ruleToMetrics;
    return metrics;
  }

  std::string toJson() const {
    auto obj = toDynamic();
    return folly::toJson(obj);
  }

  static AttributionOutputMetrics fromJson(const std::string& str) {
    auto obj = folly::parseJson(str);
    return fromDynamic(obj);
  }
};

} // namespace measurement::private_attribution
