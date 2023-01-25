#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

BYTES_PER_MEGABYTE=1000000

# We need up to 7000 MB (less than 7GB) per thread.
MAX_THREAD_MEMORY_IN_BYTES=$((7000 * BYTES_PER_MEGABYTE))

# The functionality below calculates the number of bytes of memory available by getting the number
# Of physical pages available, multiplying it by the page size (in bytes) and dividing that by the
# number of bytes per thread that we want available to give us the maximum number of threads
MAX_THREADS=$(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / MAX_THREAD_MEMORY_IN_BYTES))

# Get the number of possible threads by pulling the online processors
EFFECTIVE_THREADS=$(getconf _NPROCESSORS_ONLN)

echo "Max thread memory: $MAX_THREAD_MEMORY_IN_BYTES"
echo "Max threads: $MAX_THREADS"
echo "Effective Thread: $EFFECTIVE_THREAD"

# Use the lesser of the maximum allowed threads by memory or available processors
MAKE_JOBS=$((MAX_THREADS < EFFECTIVE_THREADS ? MAX_THREADS : EFFECTIVE_THREADS))

# Set the maximum load average on the CPU as 9/10ths of the available cores
# This ensures that we don't saturate the CPU with processes that are greater
# than the cores available
MAKE_MAX_LOAD=$((EFFECTIVE_THREADS * 9 / 10))

make -j $MAKE_JOBS -l $MAKE_MAX_LOAD && make -j $MAKE_JOBS -l $MAKE_MAX_LOAD install
