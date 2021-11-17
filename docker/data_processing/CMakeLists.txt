# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.10.1)
project(attribution)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_file(fbpcf_cmake NAMES cmake/fbpcf.cmake)
include(${fbpcf_cmake})

find_library(fbpcf libfbpcf.a)

# data processing common
file(GLOB data_processing_common_src
  "fbpcs/data_processing/common/**.c"
  "fbpcs/data_processing/common/**.cpp"
  "fbpcs/data_processing/common/**.h"
  "fbpcs/data_processing/common/**.hpp")
list(FILTER data_processing_common_src EXCLUDE REGEX ".*Test.*")
add_library(dpcommon STATIC
  ${data_processing_common_src})
target_link_libraries(
  dpcommon
  INTERFACE
  fbpcf
  ${AWSSDK_LINK_LIBRARIES}
  ${EMP-OT_LIBRARIES}
  Folly::folly
  re2)

# sharder
add_executable(
  sharder
  "fbpcs/data_processing/sharding/shard.cpp")
target_link_libraries(
  sharder
  dpcommon)
install(TARGETS sharder DESTINATION bin)

# sharder hashed for pid
file(GLOB hash_slinging_salter_src
  "fbpcs/data_processing/hash_slinging_salter/HashSlingingSalter.cpp"
  "fbpcs/data_processing/hash_slinging_salter/base64.cpp")
add_executable(
  sharder_hashed_for_pid
  "fbpcs/data_processing/sharding/shard_pid.cpp"
  ${hash_slinging_salter_src})
target_link_libraries(
  sharder_hashed_for_pid
  dpcommon)
install(TARGETS sharder_hashed_for_pid DESTINATION bin)

# pid preparer
add_executable(
  pid_preparer
  "fbpcs/data_processing/pid_preparer/UnionPIDDataPreparer.cpp"
  "fbpcs/data_processing/pid_preparer/union_pid_data_preparer.cpp")
target_link_libraries(
  pid_preparer
  dpcommon)
install(TARGETS pid_preparer DESTINATION bin)

# id combiner library
file(GLOB id_combiner_lib_src
  "fbpcs/data_processing/id_combiner/**.cpp")
add_library(idcombiner STATIC
  ${id_combiner_lib_src})
target_link_libraries(
  idcombiner
  INTERFACE
  dpcommon)

# lift id combiner
add_executable(
  lift_id_combiner
  "fbpcs/data_processing/lift_id_combiner/LiftIdSpineCombiner.cpp"
  "fbpcs/data_processing/lift_id_combiner/LiftIdSpineCombinerOptions.cpp"
  "fbpcs/data_processing/lift_id_combiner/LiftIdSpineFileCombiner.cpp")
target_link_libraries(
  lift_id_combiner
  idcombiner)
install(TARGETS lift_id_combiner DESTINATION bin)

# attribution id combiner
add_executable(
  attribution_id_combiner
  "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineCombiner.cpp"
  "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineCombinerOptions.cpp"
  "fbpcs/data_processing/attribution_id_combiner/AttributionIdSpineFileCombiner.cpp")
target_link_libraries(
  attribution_id_combiner
  idcombiner)
install(TARGETS attribution_id_combiner DESTINATION bin)
