/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include "fbpcs/emp_games/attribution/decoupled_aggregation/Aggregator.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/TouchPointMetadata.h"

namespace aggregation::private_aggregation {

//utility method used for parsing string information to vector of type T.
template <typename T>
static const std::vector<T> getInnerArray(std::string& str) {
  // Strip the brackets [] before splitting into individual timestamp values
  auto innerString = str;
  innerString.erase(
      std::remove(innerString.begin(), innerString.end(), '['),
      innerString.end());
  innerString.erase(
      std::remove(innerString.begin(), innerString.end(), ']'),
      innerString.end());
  auto innerVals = private_measurement::csv::splitByComma(innerString, false);

  std::vector<T> out;

  for (const auto& innerVal : innerVals) {
    if (!innerVal.empty()) {
      T parsed = 0;
      std::istringstream iss{innerVal};
      iss >> parsed;
      out.push_back(parsed);
    }
  }

  return out;
}

struct AggregationMetrics {
  using AttributionResultsMap =
      std::vector<std::map<int64_t, std::vector<AttributionResult>>>;
  using AttributionResultsList =
      std::vector<std::vector<std::vector<AttributionResult>>>;
  AttributionResultsList attributionPidVector;

  std::vector<std::string> attributionList;
  std::unordered_map<std::string, AggregationOutput> formatToAggregation;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();
    for (auto kv : formatToAggregation) {
      auto aggregationName = kv.first;
      auto aggregationMetrics = kv.second;
      res.insert(aggregationName, aggregationMetrics);
    }

    return res;
  }

  static AggregationMetrics fromDynamic(const folly::dynamic& obj) {
    std::unordered_map<std::string, AggregationOutput> formatToAggregation;
    for (auto& pair : obj.items()) {
      auto aggregationName = pair.first.asString();
      auto aggregationMetrics = pair.second;
      std::pair<std::string, AggregationOutput> t{
          aggregationName, aggregationMetrics};
      formatToAggregation.insert(t);
    }

    AggregationMetrics metrics{};
    metrics.formatToAggregation = formatToAggregation;

    return metrics;
  }

  // Secret share attribution result received by the game will be structured as
  // : {"rule1" -> {"format1" -> {"pid1" -> {results}}}} Thus here, we are
  // iterating over list of attribution results per pid per format per rule and
  // adding then to a vector of map. map -> {pid, vector<result>> . While
  // running aggregation game, we will share this vector of vectors between
  // parties (order maintained), where each vector would represent results for
  // one rule and one format.
  static AggregationMetrics::AttributionResultsList
  getAttributionsArrayfromDynamic(const folly::dynamic& obj) {
    AttributionResultsMap attributionPidVectorMap;
    std::vector<std::string> attributionList;
    // For now, I am not using the rule name or formatter name in the logic as
    // the aggregation behaviour is not affected by different attribution rules.
    AttributionResultsList attributionResultsList;
    for (const auto& [rule, formatters] : obj.items()) {
      attributionList.push_back(rule.asString());
      for (const auto& [formatter, resultPerPID] : formatters.items()) {
        std::map<int64_t, std::vector<AttributionResult>> attributionsPerPidMap;
        for (const auto& [pid, results] : resultPerPID.items()) {
          std::vector<AttributionResult> attributionResults;
          for (const auto& result : results) {
            attributionResults.push_back(
                AttributionResult::fromDynamic(result));
          }
          attributionsPerPidMap.emplace(pid.asInt(), attributionResults);
        }
        attributionPidVectorMap.push_back(attributionsPerPidMap);
      }

      for (const auto& attributionsPerPidMap : attributionPidVectorMap) {
        std::vector<std::vector<AttributionResult>> attributionPidVector;
        for (const auto& attributionResults : attributionsPerPidMap) {
          attributionPidVector.push_back(attributionResults.second);
        }
        attributionResultsList.push_back(attributionPidVector);
      }
    }

    return attributionResultsList;
  }
};

struct AggregationOutputMetrics {
  std::unordered_map<std::string, AggregationMetrics> ruleToMetrics;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();
    for (auto& kv : ruleToMetrics) {
      auto ruleName = kv.first;
      auto metrics = kv.second;
      res.insert(ruleName, metrics.toDynamic());
    }

    return res;
  }

  static AggregationOutputMetrics fromDynamic(const folly::dynamic& obj) {
    AggregationOutputMetrics metrics;
    std::unordered_map<std::string, AggregationMetrics> ruleToMetrics;

    for (auto& pair : obj.items()) {
      std::string ruleName = pair.first.asString();
      auto attributionMetrics = AggregationMetrics::fromDynamic(pair.second);
      std::pair<std::string, AggregationMetrics> t{
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

  static AggregationOutputMetrics fromJson(const std::string& str) {
    auto obj = folly::parseJson(str);
    return fromDynamic(obj);
  }
};

/*
 * This class represents input data for Private Aggregation.
 * It processes an input csv and generates the std::vectors for each column
 */
class AggregationInputMetrics {
 public:
  explicit AggregationInputMetrics(
      int myRole,
      std::filesystem::path inputSecretShareFilePath,
      std::filesystem::path inputClearTextFilePaths,
      std::string aggregationFormatName);

  const std::vector<int64_t>& getIds() const {
    return ids_;
  }

  const std::vector<std::string>& getAttributionRules() const {
    return attributionRules_;
  }

  const AggregationMetrics::AttributionResultsList& getTouchpointSecretShares()
      const {
    return touchpointSecretShare_;
  }

  const AggregationMetrics::AttributionResultsList& getConversionSecretShares()
      const {
    return conversionSecretShare_;
  }

  const std::vector<std::vector<TouchpointMetadata>>& getTouchpointMetadata()
      const {
    return touchpointMetadataArrays_;
  }

  const std::vector<std::vector<ConversionMetadata>>& getConversionMetadata()
      const {
    return conversiontMetadataArrays_;
  }

  const std::vector<AggregationFormat>& getAggregationFormats() const {
    return aggregationFormats_;
  }

 private:
  std::vector<int64_t> ids_;
  std::vector<std::string> attributionRules_;
  std::vector<AggregationFormat> aggregationFormats_;
  AggregationMetrics::AttributionResultsList touchpointSecretShare_;
  AggregationMetrics::AttributionResultsList conversionSecretShare_;
  std::vector<std::vector<TouchpointMetadata>> touchpointMetadataArrays_;
  std::vector<std::vector<ConversionMetadata>> conversiontMetadataArrays_;
};

class PrivateAggregationMetrics {
 public:
  PrivateAggregationMetrics(
      std::vector<AggregationFormat> aggregationFormats_,
      const AggregationContext& ctx,
      const fbpcf::Visibility& outputVisibility) {
    for (auto aggregationFormat : aggregationFormats_) {
      formatToAggregator[aggregationFormat.name] =
          aggregationFormat.newAggregator(ctx, outputVisibility);
    }
  }

  void computeAggregationsPerFormat(
      const PrivateAggregation& privateAggregation) {
    for (const auto& [format, aggregator] : formatToAggregator) {
      aggregator->aggregateAttributions(privateAggregation);
    }
  }

  AggregationMetrics reveal() {
    AggregationMetrics out;

    for (const auto& [format, aggregator] : formatToAggregator) {
      out.formatToAggregation[format] = aggregator->reveal();
    }

    return out;
  }

 private:
  std::unordered_map<std::string, std::unique_ptr<Aggregator>>
      formatToAggregator;
};

} // namespace aggregation::private_aggregation
