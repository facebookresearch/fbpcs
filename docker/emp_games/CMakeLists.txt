# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.10.1)
project(game)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include("common.cmake")

# lift
add_executable(
  lift_calculator
  "fbpmp/emp_games/lift/calculator/main.cpp"
  "fbpmp/emp_games/lift/calculator/OutputMetrics.hpp"
  "fbpmp/emp_games/lift/common/GroupedLiftMetrics.h"
  "fbpmp/emp_games/lift/common/GroupedLiftMetrics.cpp"
  "fbpmp/emp_games/lift/common/LiftMetrics.h"
  "fbpmp/emp_games/lift/common/LiftMetrics.cpp"
  "fbpmp/emp_games/lift/calculator/CalculatorApp.h"
  "fbpmp/emp_games/lift/calculator/CalculatorApp.cpp"
  "fbpmp/emp_games/lift/calculator/CalculatorGame.h"
  "fbpmp/emp_games/lift/calculator/OutputMetrics.h"
  "fbpmp/emp_games/lift/calculator/InputData.cpp"
  "fbpmp/emp_games/lift/calculator/InputData.h"
  "fbpmp/emp_games/lift/calculator/CalculatorGameConfig.h"
  "fbpmp/emp_games/lift/calculator/OutputMetricsData.h"
  "fbpmp/emp_games/common/PrivateData.h"
  "fbpmp/emp_games/common/SecretSharing.h"
  "fbpmp/emp_games/common/EmpOperationUtil.h"
  "fbpmp/emp_games/common/Csv.h"
  "fbpmp/emp_games/common/Csv.cpp")
target_link_libraries(
  lift_calculator
  empgamecommon)
install(TARGETS lift_calculator DESTINATION bin)

# attribution
file(GLOB attribution_src
  "fbpmp/emp_games/attribution/**.c"
  "fbpmp/emp_games/attribution/**.cpp"
  "fbpmp/emp_games/attribution/**.h"
  "fbpmp/emp_games/attribution/**.hpp")
list(FILTER attribution_src EXCLUDE REGEX ".*Test.*")
add_executable(
  attribution_calculator
  ${attribution_src})
target_link_libraries(
  attribution_calculator
  empgamecommon)
install(TARGETS attribution_calculator DESTINATION bin)

# generic shard_aggregator
file(GLOB shard_aggregator_src
  "fbpmp/emp_games/attribution/shard_aggregator/AggMetrics.cpp",
  "fbpmp/emp_games/attribution/shard_aggregator/AggMetricsThresholdCheckers.cpp",
  "fbpmp/emp_games/attribution/shard_aggregator/ShardAggregatorApp.cpp",
  "fbpmp/emp_games/attribution/shard_aggregator/ShardAggregatorValidation.cpp",
  "fbpmp/emp_games/attribution/shard_aggregator/main.cpp"
  "fbpmp/emp_games/attribution/Aggregator.h"
  "fbpmp/emp_games/attribution/AttributionMetrics.h"
  "fbpmp/emp_games/attribution/AttributionRule.h"
  "fbpmp/emp_games/attribution/Constants.h"
  "fbpmp/emp_games/attribution/Conversion.h"
  "fbpmp/emp_games/attribution/Debug.h"
  "fbpmp/emp_games/attribution/Timestamp.h",
  "fbpmp/emp_games/attribution/Touchpoint.h",
  "fbpmp/emp_games/attribution/shard_aggregator/MainUtil.h",
  "fbpmp/emp_games/attribution/shard_aggregator/ShardAggregatorGame.h")
add_executable(
  shard_aggregator
  ${shard_aggregator_src})
target_link_libraries(
  shard_aggregator
  empgamecommon)
install(TARGETS shard_aggregator DESTINATION bin)
