# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.16)
project(game)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_library(range_v3 INTERFACE IMPORTED)
set_target_properties(range_v3 PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES /root/build/range-v3/include)
find_program(CMAKE_CXX_COMPILER clang++)

# Don't compile with AVX512 instructions since many of the AWS
# instances won't have access to that instruction set.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mno-avx512f -march=haswell")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mno-avx512f -march=haswell")

include("common.cmake")
include("perf_tools.cmake")

message(STATUS "CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}")
message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_LINKER: ${CMAKE_LINKER}")

# pcf2_udp encryption
file(GLOB udp_encryptor_src
  "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/**.cpp"
  "fbpcs/emp_games/data_processing/unified_data_process/UdpEncryptor/**.h")
list(FILTER udp_encryptor_src EXCLUDE REGEX ".*Test.*")
add_executable(
  udp_encryptor
  ${udp_encryptor_src})
target_link_libraries(
  udp_encryptor
  empgamecommon
  perftools
  ${FOLLY_LIBRARIES}
  ${Boost_LIBRARIES}
  range_v3
)
install(TARGETS udp_encryptor DESTINATION bin)
