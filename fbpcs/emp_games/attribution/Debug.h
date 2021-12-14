/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

constexpr bool omniscientModeEnabled = false;

// Ensure omniscient mode never makes it into production.
#ifdef NDEBUG
static_assert(
    !omniscientModeEnabled,
    "Omniscient mode should only be enabled on debug builds.");
#endif

// If block that ensures the block is only included in the final binary if
// OMNISCIENT_MODE_ENABLED evaluates to true
#define IF_OMNISCIENT_MODE if constexpr (omniscientModeEnabled)

// folly XLOGF that is only included in the final binary if
// OMNISCIENT_MODE_ENABLED evaluates to true
#define OMNISCIENT_ONLY_XLOGF(level, fmt, arg1, ...) \
  IF_OMNISCIENT_MODE {                               \
    XLOGF(level, fmt, arg1, ##__VA_ARGS__);          \
  }

// folly XLOG that is only included in the final binary if
// OMNISCIENT_MODE_ENABLED evaluates to true
#define OMNISCIENT_ONLY_XLOG(level, ...) \
  IF_OMNISCIENT_MODE {                   \
    XLOG(level, ##__VA_ARGS__);          \
  }
