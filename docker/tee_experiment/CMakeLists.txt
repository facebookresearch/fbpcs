# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.10.1)
project(tee_experiment)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include("common.cmake")
include("perf_tools.cmake")

# plain_text_lift_calculator
add_executable(
  plain_text_lift_calculator
  "fbpcs/emp_games/lift/calculator/test/main.cpp"
  "fbpcs/emp_games/lift/calculator/test/common/LiftCalculator.h"
  "fbpcs/emp_games/lift/calculator/test/common/LiftCalculator.cpp"
  "fbpcs/emp_games/lift/calculator/OutputMetrics.hpp"
  "fbpcs/emp_games/lift/calculator/OutputMetrics.h"
  "fbpcs/emp_games/lift/calculator/InputData.cpp"
  "fbpcs/emp_games/lift/calculator/InputData.h"
  "fbpcs/emp_games/lift/calculator/OutputMetricsData.h"
  "fbpcs/emp_games/common/Csv.h"
  "fbpcs/emp_games/common/Csv.cpp"
)
target_link_libraries(
  plain_text_lift_calculator
  empgamecommon
)
install(TARGETS plain_text_lift_calculator DESTINATION bin)
