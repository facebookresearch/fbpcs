# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.10.1)
project(game)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include("common.cmake")
include("perf_tools.cmake")

# common
file(GLOB common_src
  "fbpcs/emp_games/common/**.c"
  "fbpcs/emp_games/common/**.cpp"
  "fbpcs/emp_games/common/**.h"
  "fbpcs/emp_games/common/**.hpp"
)
list(FILTER common_src EXCLUDE REGEX ".*Test.*")

# lift
file(GLOB lift_src
  "fbpcs/emp_games/lift/**.c"
  "fbpcs/emp_games/lift/**.cpp"
  "fbpcs/emp_games/lift/**.h"
  "fbpcs/emp_games/lift/**.hpp")
list(FILTER lift_src EXCLUDE REGEX ".*Test.*")
add_executable(
  lift_calculator
  ${common_src}
  ${lift_src})
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
  empgamecommon
  perftools)
install(TARGETS attribution_calculator DESTINATION bin)

# generic shard_aggregator
file(GLOB shard_aggregator_src
  "fbpcs/emp_games/attribution/shard_aggregator/**.c"
  "fbpcs/emp_games/attribution/shard_aggregator/**.cpp"
  "fbpcs/emp_games/attribution/shard_aggregator/**.h"
  "fbpcs/emp_games/attribution/shard_aggregator/**.hpp")
list(FILTER shard_aggregator_src EXCLUDE REGEX ".*Test.*")
add_executable(
  shard_aggregator
  ${attribution_src}
  ${shard_aggregator_src})
target_link_libraries(
  shard_aggregator
  empgamecommon
  perftools)
install(TARGETS shard_aggregator DESTINATION bin)

# decoupled_attribution
file(GLOB decoupled_attribution_src
  "fbpcs/emp_games/attribution/decoupled_attribution/**.c"
  "fbpcs/emp_games/attribution/decoupled_attribution/**.cpp"
  "fbpcs/emp_games/attribution/decoupled_attribution/**.h"
  "fbpcs/emp_games/attribution/decoupled_attribution/**.hpp")
list(FILTER decoupled_attribution_src EXCLUDE REGEX ".*Test.*")
add_executable(
  decoupled_attribution_calculator
  ${decoupled_attribution_src})
target_link_libraries(
  decoupled_attribution_calculator
  empgamecommon
  perftools)
install(TARGETS decoupled_attribution_calculator DESTINATION bin)

# decoupled_aggregation
file(GLOB decoupled_aggregation_src
  "fbpcs/emp_games/attribution/decoupled_aggregation/**.c"
  "fbpcs/emp_games/attribution/decoupled_aggregation/**.cpp"
  "fbpcs/emp_games/attribution/decoupled_aggregation/**.h"
  "fbpcs/emp_games/attribution/decoupled_aggregation/metadata/**.h"
  "fbpcs/emp_games/attribution/decoupled_aggregation/**.hpp")
list(FILTER decoupled_aggregation_src EXCLUDE REGEX ".*Test.*")
add_executable(
  decoupled_aggregation_calculator
  ${decoupled_aggregation_src})
target_link_libraries(
  decoupled_aggregation_calculator
  empgamecommon
  perftools)
install(TARGETS decoupled_aggregation_calculator DESTINATION bin)
