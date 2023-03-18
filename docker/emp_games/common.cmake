# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

find_file(fbpcf_cmake NAMES cmake/fbpcf.cmake)
include(${fbpcf_cmake})

find_library(fbpcf libfbpcf.a)

# Find boost
find_package(Boost COMPONENTS serialization REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})


# emp game common
file(GLOB emp_game_common_src
  "fbpcs/emp_games/common/**.c"
  "fbpcs/emp_games/common/**.cpp"
  "fbpcs/emp_games/common/**.h"
  "fbpcs/emp_games/common/**.hpp"
  "fbpcs/emp_games/data_processing/global_parameters/**.cpp"
  "fbpcs/emp_games/data_processing/global_parameters/**.h"
  )
list(FILTER emp_game_common_src EXCLUDE REGEX ".*Test.*")
add_library(empgamecommon STATIC
  ${emp_game_common_src})
target_link_libraries(
  empgamecommon
  INTERFACE
  fbpcf
  ${Boost_LIBRARIES}
  ${AWSSDK_LINK_LIBRARIES}
  ${EMP-OT_LIBRARIES}
  google-cloud-cpp::storage
  ${FOLLY_LIBRARIES}
  re2)

# pcf2 lift input processing
file(GLOB_RECURSE pcf2_lift_input_processing_src
  "fbpcs/emp_games/lift/pcf2_calculator/input_processing/**.cpp"
  "fbpcs/emp_games/lift/pcf2_calculator/input_processing/**.h")
list(FILTER pcf2_lift_input_processing_src EXCLUDE REGEX ".*Test.*")
add_library(pcf2_lift_input_processing STATIC
  ${pcf2_lift_input_processing_src})
target_link_libraries(
  pcf2_lift_input_processing
  INTERFACE
  empgamecommon
)
