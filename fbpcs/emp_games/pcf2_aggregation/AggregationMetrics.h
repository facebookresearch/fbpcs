/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include "fbpcs/emp_games/common/Csv.h"

#include "fbpcs/emp_games/pcf2_aggregation/Aggregator.h"
#include "fbpcs/emp_games/pcf2_aggregation/AttributionReformattedResult.h"
#include "fbpcs/emp_games/pcf2_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/pcf2_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/pcf2_aggregation/TouchpointMetadata.h"

namespace pcf2_aggregation {

struct AggregationMetrics {
  using AttributionResultsMap =
      std::vector<std::map<int64_t, std::vector<AttributionResult>>>;
  using AttributionResultsList =
      std::vector<std::vector<std::vector<AttributionResult>>>;

  using AttributionReformattedResultsMap =
      std::vector<std::map<int64_t, std::vector<AttributionReformattedResult>>>;
  using AttributionReformattedResultsList =
      std::vector<std::vector<std::vector<AttributionReformattedResult>>>;

  AttributionResultsList attributionPidVector;
  AttributionReformattedResultsList attributionReformattedPidVector;

  std::vector<std::string> attributionList;
  std::vector<std::string> attributionReformattedList;
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
  // adding them to a vector of maps from pid to vector<result>. While
  // running the aggregation game, we will share this vector of vectors between
  // parties (order maintained), where each vector would represent results for
  // one rule and one format.
  static AggregationMetrics::AttributionResultsList
  getAttributionsArrayfromDynamic(const folly::dynamic& obj) {
    AttributionResultsMap attributionPidVectorMap;
    std::vector<std::string> attributionList; // list of attribution rules
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

  static AggregationMetrics::AttributionReformattedResultsList
  getAttributionsReformattedArrayfromDynamic(const folly::dynamic& obj) {
    AttributionReformattedResultsMap attributionReformattedPidVectorMap;
    std::vector<std::string>
        attributionReformattedList; // list of attribution rules
    // For now, I am not using the rule name or formatter name in the logic as
    // the aggregation behaviour is not affected by different attribution rules.
    AttributionReformattedResultsList attributionReformattedResultsList;
    for (const auto& [rule, formatters] : obj.items()) {
      attributionReformattedList.push_back(rule.asString());
      for (const auto& [formatter, resultPerPID] : formatters.items()) {
        std::map<int64_t, std::vector<AttributionReformattedResult>>
            attributionsReformattedPerPidMap;
        for (const auto& [pid, results] : resultPerPID.items()) {
          std::vector<AttributionReformattedResult>
              attributionReformattedResults;
          for (const auto& result : results) {
            attributionReformattedResults.push_back(
                AttributionReformattedResult::fromDynamic(result));
          }
          attributionsReformattedPerPidMap.emplace(
              pid.asInt(), attributionReformattedResults);
        }
        attributionReformattedPidVectorMap.push_back(
            attributionsReformattedPerPidMap);
      }

      for (const auto& attributionsPerPidMap :
           attributionReformattedPidVectorMap) {
        std::vector<std::vector<AttributionReformattedResult>>
            attributionPidVector;
        for (const auto& attributionResults : attributionsPerPidMap) {
          attributionPidVector.push_back(attributionResults.second);
        }
        attributionReformattedResultsList.push_back(attributionPidVector);
      }
    }

    return attributionReformattedResultsList;
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
      common::InputEncryption inputEncryption,
      std::filesystem::path inputSecretShareFilePath,
      std::filesystem::path inputClearTextFilePaths,
      std::string aggregationFormatName);

  explicit AggregationInputMetrics(
      std::vector<int64_t> ids,
      std::vector<std::string> attributionRules,
      std::vector<std::string> aggregationFormats,
      AggregationMetrics::AttributionResultsList attributionSecretShare,
      AggregationMetrics::AttributionReformattedResultsList
          attributionReformattedSecretShare,
      std::vector<std::vector<TouchpointMetadata>> touchpointMetadataArrays,
      std::vector<std::vector<ConversionMetadata>> conversionMetadataArrays)
      : ids_{ids},
        attributionRules_{attributionRules},
        aggregationFormats_{aggregationFormats},
        attributionSecretShare_{attributionSecretShare},
        attributionReformattedSecretShare_{attributionReformattedSecretShare},
        touchpointMetadataArrays_{touchpointMetadataArrays},
        conversionMetadataArrays_{conversionMetadataArrays} {}

  const std::vector<int64_t>& getIds() const {
    return ids_;
  }

  const std::vector<std::string>& getAttributionRules() const {
    return attributionRules_;
  }

  const AggregationMetrics::AttributionResultsList& getAttributionSecretShares()
      const {
    return attributionSecretShare_;
  }

  const AggregationMetrics::AttributionReformattedResultsList&
  getAttributionReformattedSecretShares() const {
    return attributionReformattedSecretShare_;
  }

  const std::vector<std::vector<TouchpointMetadata>>& getTouchpointMetadata()
      const {
    return touchpointMetadataArrays_;
  }

  const std::vector<std::vector<ConversionMetadata>>& getConversionMetadata()
      const {
    return conversionMetadataArrays_;
  }

  const std::vector<std::string>& getAggregationFormats() const {
    return aggregationFormats_;
  }

 private:
  std::vector<int64_t> ids_;
  std::vector<std::string> attributionRules_;
  std::vector<std::string> aggregationFormats_;
  AggregationMetrics::AttributionResultsList attributionSecretShare_;
  AggregationMetrics::AttributionReformattedResultsList
      attributionReformattedSecretShare_;
  std::vector<std::vector<TouchpointMetadata>> touchpointMetadataArrays_;
  std::vector<std::vector<ConversionMetadata>> conversionMetadataArrays_;
};

template <int schedulerId>
class PrivateAggregationMetrics {
 public:
  PrivateAggregationMetrics(
      std::vector<AggregationFormat<schedulerId>> aggregationFormats_,
      const AggregationContext& ctx,
      const int myRole,
      const int concurrency,
      std::unique_ptr<fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<
          fbpcf::mpc_std_lib::util::AggregationValue>> writeOnlyOramFactory) {
    for (auto aggregationFormat : aggregationFormats_) {
      formatToAggregator[aggregationFormat.name] =
          aggregationFormat.newAggregator(
              ctx, myRole, concurrency, std::move(writeOnlyOramFactory));
    }
  }

  void computeAggregationsPerFormat(
      const PrivateAggregation<schedulerId>& privateAggregation) {
    for (const auto& [format, aggregator] : formatToAggregator) {
      aggregator->aggregateAttributions(privateAggregation);
    }
  }

  void computeAggregationsReformattedPerFormat(
      const PrivateAggregationReformatted<schedulerId>&
          privateAggregationReformatted) {
    for (const auto& [format, aggregator] : formatToAggregator) {
      aggregator->aggregateReformattedAttributions(
          privateAggregationReformatted);
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
  std::unordered_map<std::string, std::unique_ptr<Aggregator<schedulerId>>>
      formatToAggregator;
};

} // namespace pcf2_aggregation
