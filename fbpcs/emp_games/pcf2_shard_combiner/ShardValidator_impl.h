/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

// checks if the `test_param` meets the `expected` based on the
// `operator` otherwise throws a said expection `throw_` with a message
// `msg`.
#define VALIDATE_OR_THROW(test_param, operator, expected, throw_, msg) \
  do {                                                                 \
    if (test_param operator expected) {                                \
    } else {                                                           \
      throw throw_(msg);                                               \
    }                                                                  \
  } while (0)

#include <fbpcf/exception/exceptions.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/AggMetrics.h>
#include <fbpcs/emp_games/pcf2_shard_combiner/ShardValidator.h>

namespace shard_combiner {

template <
    ShardSchemaType shardSchemaType,
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void validateShardSchema(
    const AggMetrics<schedulerId, usingBatch, inputEncryption>& metrics) {
  if constexpr (shardSchemaType == ShardSchemaType::kAdObjFormat) {
    validateAdObjectFormatMetrics(metrics);
  } else if constexpr (
      shardSchemaType == ShardSchemaType::kGroupedLiftMetrics) {
    validateGroupedLiftMetrics(metrics);
  } else {
    throw common::exceptions::SchemaTraceError(folly::sformat(
        "This [{}] schema is currently not supported in pcf2_shard_combiner.",
        shardSchemaType));
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void validateAdObjectFormatMetrics(
    const AggMetrics<schedulerId, usingBatch, inputEncryption>& metrics) {
  VALIDATE_OR_THROW(
      metrics.getAsDict().size(),
      >,
      0,
      common::exceptions::SchemaTraceError,
      "Metrics cannot have an empty dictionary.");
  for (const auto& [rule, metricsEntry] : metrics.getAsDict()) {
    VALIDATE_OR_THROW(
        metricsEntry->getType(),
        ==,
        AggMetricType::kDict,
        common::exceptions::SchemaTraceError,
        folly::sformat("Metrics rule: {} should be a dictionary.", rule));

    VALIDATE_OR_THROW(
        metricsEntry->getAsDict().size(),
        >,
        0,
        common::exceptions::SchemaTraceError,
        folly::sformat(
            "Metrics rule: {} should be a dictionary of size > 0.", rule));

    for (const auto& [aggregationName, aggregationData] :
         metricsEntry->getAsDict()) {
      VALIDATE_OR_THROW(
          aggregationName,
          ==,
          "measurement",
          common::exceptions::SchemaTraceError,
          folly::sformat(
              "Unsupported aggregationName [{}] passed to Shard Aggregator",
              aggregationName));
      VALIDATE_OR_THROW(
          aggregationData->getType(),
          ==,
          AggMetricType::kDict,
          common::exceptions::SchemaTraceError,
          folly::sformat(
              "Aggregation should be a Dictionary({}), got: [{}]",
              (int)AggMetricType::kDict,
              (int)aggregationData->getType()));
    }
  }
}

template <
    int schedulerId,
    bool usingBatch,
    common::InputEncryption inputEncryption>
void validateGroupedLiftMetrics(
    const AggMetrics<schedulerId, usingBatch, inputEncryption>& metrics) {
  VALIDATE_OR_THROW(
      metrics.getType(),
      ==,
      AggMetricType::kDict,
      common::exceptions::SchemaTraceError,
      folly::sformat(
          "GroupedLiftMetrics expects dictionary as input, got: [{}]",
          (int)metrics.getType()));
  VALIDATE_OR_THROW(
      metrics.getAsDict().find("metrics"),
      !=,
      metrics.getAsDict().end(),
      common::exceptions::SchemaTraceError,
      "Dict does not have 'metrics' key");
  VALIDATE_OR_THROW(
      metrics.getAsDict().find("cohortMetrics"),
      !=,
      metrics.getAsDict().end(),
      common::exceptions::SchemaTraceError,
      "Dict does not have 'cohortMetrics' key, maybe SchemaType is wrong?");
  VALIDATE_OR_THROW(
      metrics.getAsDict().find("publisherBreakdowns"),
      !=,
      metrics.getAsDict().end(),
      common::exceptions::SchemaTraceError,
      "Dict does not have 'publisherBreakdowns' key");
}
} // namespace shard_combiner
