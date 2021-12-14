# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

find_file(fbpcf_cmake NAMES cmake/fbpcf.cmake)
include(${fbpcf_cmake})

find_library(fbpcf libfbpcf.a)

# perf_tools
file(GLOB perf_tools_src
  "fbpcs/performance_tools/**.cpp"
  "fbpcs/performance_tools/**.h")
list(FILTER perf_tools_src EXCLUDE REGEX ".*Test.*")
add_library(perftools STATIC
  ${perf_tools_src})
target_link_libraries(
  perftools
  INTERFACE
  fbpcf
  ${AWSSDK_LINK_LIBRARIES}
  ${EMP-OT_LIBRARIES}
  Folly::folly
  re2)
