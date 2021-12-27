# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

find_file(fbpcf_cmake NAMES cmake/fbpcf.cmake)
include(${fbpcf_cmake})

find_library(fbpcf libfbpcf.a)

# emp game common
file(GLOB emp_game_common_src
  "fbpcs/emp_games/common/**.c"
  "fbpcs/emp_games/common/**.cpp"
  "fbpcs/emp_games/common/**.h"
  "fbpcs/emp_games/common/**.hpp")
list(FILTER emp_game_common_src EXCLUDE REGEX ".*Test.*")
add_library(empgamecommon STATIC
  ${emp_game_common_src})
target_link_libraries(
  empgamecommon
  INTERFACE
  fbpcf
  ${AWSSDK_LINK_LIBRARIES}
  ${EMP-OT_LIBRARIES}
  Folly::folly
  re2)
