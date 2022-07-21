/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <math.h>
#include <memory>
#include "folly/json.h"
#include "folly/logging/xlog.h"

#include "fbpcf/engine/communication/IPartyCommunicationAgentFactory.h"
#include "fbpcf/mpc_std_lib/oram/IWriteOnlyOramFactory.h"
#include "fbpcf/mpc_std_lib/util/util.h"
#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/pcf2_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/pcf2_aggregation/Constants.h"
#include "fbpcs/emp_games/pcf2_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/pcf2_aggregation/TouchpointMetadata.h"

namespace pcf2_aggregation {

using AttributionResultsList =
    std::vector<std::vector<std::vector<AttributionResult>>>;

template <int schedulerId>
using MeasurementTpmArrays =
    std::vector<std::vector<PrivateMeasurementTouchpointMetadata<schedulerId>>>;

template <int schedulerId>
using MeasurementCvmArrays =
    std::vector<std::vector<PrivateMeasurementConversionMetadata<schedulerId>>>;

using AggregationOutput = folly::dynamic;

template <int schedulerId>
struct PrivateAggregation {
  std::vector<std::vector<PrivateAttributionResult<schedulerId>>>
      attributionResults;
  MeasurementTpmArrays<schedulerId> privateTpm;
  MeasurementCvmArrays<schedulerId> privateCvm;
  // TODO: Add fields for additional aggregators to PrivateAggregation.
};

struct ConvMetrics {
  uint32_t convs;
  uint32_t sales;

  folly::dynamic toDynamic() const {
    return folly::dynamic::object("sales", sales)("convs", convs);
  }

  static ConvMetrics fromDynamic(const folly::dynamic& obj) {
    ConvMetrics out = ConvMetrics{};
    out.sales = obj["sales"].asInt();
    out.convs = obj["convs"].asInt();
    return out;
  }
};

template <int schedulerId>
class Aggregator {
 public:
  explicit Aggregator() {}

  virtual ~Aggregator() {}

  virtual void aggregateAttributions(
      const PrivateAggregation<schedulerId>& privateAggregation) = 0;

  virtual AggregationOutput reveal() const = 0;
};

struct AggregationContext {
  const std::vector<uint64_t>& validOriginalAdIds;
};

template <int schedulerId>
class AggregationFormat {
 public:
  uint16_t id;
  std::string name;
  Aggregator<schedulerId>& getAggregator();

  std::function<std::unique_ptr<Aggregator<schedulerId>>(
      AggregationContext,
      int myRole,
      int concurrency,
      std::unique_ptr<fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<
          fbpcf::mpc_std_lib::util::AggregationValue>> writeOnlyOramFactory)>
      newAggregator;

  static const AggregationFormat fromNameOrThrow(const std::string& name);
  static const AggregationFormat fromIdOrThrow(int64_t id);
};

} // namespace pcf2_aggregation

#include "fbpcs/emp_games/pcf2_aggregation/Aggregator_impl.h"
