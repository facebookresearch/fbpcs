/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>
#include <folly/json.h>
#include <filesystem>

#include "fbpcs/emp_games/common/Csv.h"

#include "fbpcs/emp_games/pcf2_attribution/AttributionOutput.h"
#include "fbpcs/emp_games/pcf2_attribution/Conversion.h"
#include "fbpcs/emp_games/pcf2_attribution/Touchpoint.h"

namespace pcf2_attribution {

/*
 * This class represents input data for a Private Attribution computation.
 * It processes an input csv and generates the std::vectors for each column
 */
template <bool usingBatch, common::InputEncryption inputEncryption>
class AttributionInputMetrics {
 public:
  // Constructor -- input is a path to a CSV
  explicit AttributionInputMetrics(
      int myRole,
      std::string attributionRulesStr,
      std::filesystem::path filepath);

  explicit AttributionInputMetrics(
      const std::vector<int64_t>& ids,
      const std::vector<std::string>& attributionRules,
      const std::vector<TouchpointT<usingBatch>>& tpArrays,
      const std::vector<ConversionT<usingBatch>>& convArrays)
      : ids_{ids},
        attributionRules_{attributionRules},
        tpArrays_{tpArrays},
        convArrays_{convArrays} {}

  const std::vector<int64_t>& getIds() const {
    return ids_;
  }

  const std::vector<std::string>& getAttributionRules() const {
    return attributionRules_;
  }

  const std::vector<ConversionT<usingBatch>>& getConversionArrays() const {
    return convArrays_;
  }

  const std::vector<TouchpointT<usingBatch>>& getTouchpointArrays() const {
    return tpArrays_;
  }

 private:
  std::vector<int64_t> ids_;
  std::vector<std::string> attributionRules_;
  std::vector<TouchpointT<usingBatch>> tpArrays_;
  std::vector<ConversionT<usingBatch>> convArrays_;

  /**
   * Parse touchpoints and add padding if necessary.
   */
  const std::vector<ParsedTouchpoint> parseTouchpoints(
      const int myRole,
      const int lineNo,
      const std::vector<std::string>& header,
      const std::vector<std::string>& parts);

  /**
   * Parse conversions and add padding if necessary.
   */
  const std::vector<ParsedConversion> parseConversions(
      const int myRole,
      const std::vector<std::string>& header,
      const std::vector<std::string>& parts);

  /**
   * Convert parsed touchpoints into touchpoints.
   */
  const std::vector<TouchpointT<usingBatch>>
  convertParsedTouchpointsToTouchpoints(
      const std::vector<std::vector<ParsedTouchpoint>>& parsedTouchpoints);

  /**
   * Convert parsed conversions into conversions.
   */
  const std::vector<ConversionT<usingBatch>>
  convertParsedConversionsToConversions(
      const std::vector<std::vector<ParsedConversion>>& parsedConversions);
};

/*
 * This class stores the attribution results for each attribution format.
 */
struct AttributionMetrics {
  std::unordered_map<std::string, AttributionResult> formatToAttribution;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();
    for (auto kv : formatToAttribution) {
      auto attributionName = kv.first;
      auto attributionMetrics = kv.second;
      res.insert(attributionName, attributionMetrics);
    }
    return res;
  }

  static AttributionMetrics fromDynamic(const folly::dynamic& obj) {
    std::unordered_map<std::string, AttributionResult> formatToAttribution;
    for (auto& pair : obj.items()) {
      auto attributionName = pair.first.asString();
      auto attributionMetrics = pair.second;
      std::pair<std::string, AttributionResult> t{
          attributionName, attributionMetrics};
      formatToAttribution.insert(t);
    }

    AttributionMetrics metrics{};
    metrics.formatToAttribution = formatToAttribution;

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

/*
 * This class represents output data for a Private Attribution computation.
 * It stores the output for each attribution rule.
 */
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
    return folly::toPrettyJson(obj);
  }

  static AttributionOutputMetrics fromJson(const std::string& str) {
    auto obj = folly::parseJson(str);
    return fromDynamic(obj);
  }
};

} // namespace pcf2_attribution

#include "fbpcs/emp_games/pcf2_attribution/AttributionMetrics_impl.h"
