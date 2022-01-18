/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/mpc/EmpGame.h>
#include <math.h>
#include <memory>
#include <vector>
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/Constants.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/TouchPointMetadata.h"
#include "fbpcs/emp_games/common/Csv.h"
#include "folly/json.h"
#include "folly/logging/xlog.h"

namespace aggregation::private_aggregation {

using AttributionResultsList =
    std::vector<std::vector<std::vector<AttributionResult>>>;
using MeasurementTpmArrays =
    std::vector<std::vector<PrivateMeasurementTouchpointMetadata>>;
using MeasurementCvmArrays =
    std::vector<std::vector<PrivateMeasurementConversionMetadata>>;

using AggregationOutput = folly::dynamic;

struct PrivateAggregation {
  std::vector<std::vector<PrivateAttributionResult>> tpAttributionResults;
  std::vector<std::vector<PrivateAttributionResult>> convAttributionResults;
  MeasurementTpmArrays privateTpm;
  MeasurementCvmArrays privateCvm;
  // TODO: Add fields for additional aggregatiors to PrivateAggregation.
};

struct ConvMetrics {
  int64_t convs;
  int64_t sales;

  folly::dynamic toDynamic() const {
    return folly::dynamic::object("convs", convs)("sales", sales);
  }

  static ConvMetrics fromDynamic(const folly::dynamic& obj) {
    ConvMetrics out = ConvMetrics{};
    out.convs = obj["convs"].asInt();
    out.sales = obj["sales"].asInt();
    return out;
  }
};

struct PrivateConvMetrics {
  emp::Integer convs{INT_SIZE_32, 0, emp::PUBLIC};
  emp::Integer sales{INT_SIZE_32, 0, emp::PUBLIC};

  ConvMetrics reveal(fbpcf::Visibility outputVisibility) const {
    int32_t party = static_cast<int32_t>(outputVisibility);

    return ConvMetrics{
        convs.reveal<int64_t>(party), sales.reveal<int64_t>(party)};
  }

  PrivateConvMetrics operator^(const PrivateConvMetrics& other) const noexcept {
    PrivateConvMetrics out;
    out.convs = convs ^ other.convs;
    out.sales = sales ^ other.sales;
    return out;
  }

  PrivateConvMetrics operator+(const PrivateConvMetrics& other) const noexcept {
    PrivateConvMetrics out;
    out.convs = convs + other.convs;
    out.sales = sales + other.sales;
    return out;
  }
};

class Aggregator {
 public:
  explicit Aggregator(const fbpcf::Visibility& outputVisibility)
      : outputVisibility_{outputVisibility} {}

  virtual ~Aggregator() {}

  virtual void aggregateAttributions(
      const PrivateAggregation& privateAggregation) = 0;

  virtual AggregationOutput reveal() const = 0;

 protected:
  const fbpcf::Visibility outputVisibility_;
  std::vector<int64_t> validOriginalAdIds_;
};

struct AggregationContext {
  const std::vector<int64_t>& validAdIds;
};

class AggregationFormat {
 public:
  int16_t id;
  std::string name;
  Aggregator& getAggregator();

  std::function<
      std::unique_ptr<Aggregator>(AggregationContext, fbpcf::Visibility)>
      newAggregator;
};

AggregationFormat getAggregationFormatFromNameOrThrow(const std::string& name);
AggregationFormat getAggregationFormatFromIdOrThrow(int64_t id);

} // namespace aggregation::private_aggregation
