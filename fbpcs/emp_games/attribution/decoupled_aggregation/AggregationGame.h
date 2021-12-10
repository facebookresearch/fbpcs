/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/mpc/EmpApp.h>
#include <fbpcf/mpc/EmpGame.h>
#include "fbpcs/emp_games/attribution/decoupled_aggregation/Aggregation.hpp"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/AggregationMetrics.h"
#include "fbpcs/emp_games/common/PrivateData.h"
#include "fbpcs/emp_games/common/SecretSharing.h"

namespace aggregation::private_aggregation {

template <int MY_ROLE, class IOChannel, fbpcf::Visibility OUTPUT_VISIBILITY>
class AggregationGame : public fbpcf::EmpGame<
                            IOChannel,
                            AggregationInputMetrics,
                            AggregationOutputMetrics> {
 public:
  AggregationGame(std::unique_ptr<IOChannel> ioChannel, fbpcf::Party party)
      : fbpcf::EmpGame<
            IOChannel,
            AggregationInputMetrics,
            AggregationOutputMetrics>(std::move(ioChannel), party) {}

  AggregationOutputMetrics play(
      const AggregationInputMetrics& inputData) override {
    XLOG(INFO, "Running private aggregation");
    AggregationOutputMetrics outputMetrics =
        computeAggregations<MY_ROLE>(inputData, OUTPUT_VISIBILITY);
    XLOGF(
        INFO,
        "Done. Output: {}",
        folly::toPrettyJson(outputMetrics.toDynamic()));
    return outputMetrics;
  }
};

} // namespace aggregation::private_aggregation
