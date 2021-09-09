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
  "fbpcs/emp_games/lift/calculator/main.cpp"
  "fbpcs/emp_games/lift/calculator/OutputMetrics.hpp"
  "fbpcs/emp_games/lift/common/GroupedLiftMetrics.h"
  "fbpcs/emp_games/lift/common/GroupedLiftMetrics.cpp"
  "fbpcs/emp_games/lift/common/LiftMetrics.h"
  "fbpcs/emp_games/lift/common/LiftMetrics.cpp"
  "fbpcs/emp_games/lift/calculator/CalculatorApp.h"
  "fbpcs/emp_games/lift/calculator/CalculatorApp.cpp"
  "fbpcs/emp_games/lift/calculator/CalculatorGame.h"
  "fbpcs/emp_games/lift/calculator/OutputMetrics.h"
  "fbpcs/emp_games/lift/calculator/InputData.cpp"
  "fbpcs/emp_games/lift/calculator/InputData.h"
  "fbpcs/emp_games/lift/calculator/CalculatorGameConfig.h"
  "fbpcs/emp_games/lift/calculator/OutputMetricsData.h"
  "fbpcs/emp_games/common/PrivateData.h"
  "fbpcs/emp_games/common/SecretSharing.h"
  "fbpcs/emp_games/common/EmpOperationUtil.h"
  "fbpcs/emp_games/common/Csv.h"
  "fbpcs/emp_games/common/Csv.cpp")
target_link_libraries(
  lift_calculator
  empgamecommon)
install(TARGETS lift_calculator DESTINATION bin)

# attribution
file(GLOB attribution_src
  "fbpcs/emp_games/attribution/**.c"
  "fbpcs/emp_games/attribution/**.cpp"
  "fbpcs/emp_games/attribution/**.h"
  "fbpcs/emp_games/attribution/**.hpp")
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
  "fbpcs/emp_games/attribution/shard_aggregator/AggMetrics.cpp",
  "fbpcs/emp_games/attribution/shard_aggregator/AggMetricsThresholdCheckers.cpp",
  "fbpcs/emp_games/attribution/shard_aggregator/ShardAggregatorApp.cpp",
  "fbpcs/emp_games/attribution/shard_aggregator/ShardAggregatorValidation.cpp",
  "fbpcs/emp_games/attribution/shard_aggregator/main.cpp"
  "fbpcs/emp_games/attribution/Aggregator.h"
  "fbpcs/emp_games/attribution/AttributionMetrics.h"
  "fbpcs/emp_games/attribution/AttributionRule.h"
  "fbpcs/emp_games/attribution/Constants.h"
  "fbpcs/emp_games/attribution/Conversion.h"
  "fbpcs/emp_games/attribution/Debug.h"
  "fbpcs/emp_games/attribution/Timestamp.h",
  "fbpcs/emp_games/attribution/Touchpoint.h",
  "fbpcs/emp_games/attribution/shard_aggregator/MainUtil.h",
  "fbpcs/emp_games/attribution/shard_aggregator/ShardAggregatorGame.h")
add_executable(
  shard_aggregator
  ${shard_aggregator_src})
target_link_libraries(
  shard_aggregator
  empgamecommon)
install(TARGETS shard_aggregator DESTINATION bin)
