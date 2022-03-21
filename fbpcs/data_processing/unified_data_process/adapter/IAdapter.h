/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdint.h>
#include <vector>

namespace unified_data_process::adapter {

/*
 * An adapter converts a union-like mapping result into an intersection-like
 * mapping result.
 */
class IAdapter {
 public:
  virtual ~IAdapter() = default;

  /**
   * convert a union-like mapping result to an intersection-like mapping
   * result.
   * @param unionMap the union-like mapping result, no mapping is represented
   * as -1. The i-th index in the input represents the index of this party's
   * element that corresponds to the i-th element in the union.
   * @return the corresponding intersection-like mapping result. The i-th index
   * in the output represents the index of peer's element that corresponds to
   * the i-th element in the intersection.
   */
  virtual std::vector<int64_t> adapt(
      const std::vector<int64_t>& unionMap) const = 0;
};

} // namespace unified_data_process::adapter
