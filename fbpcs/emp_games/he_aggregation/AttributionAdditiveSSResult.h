/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>
/*
 * This struct represents Additive secret shares after attribution stage
 */
namespace pcf2_he {

struct AttributionAdditiveSSResult {
  const int64_t isAttributed;

  static AttributionAdditiveSSResult fromDynamic(const folly::dynamic& obj) {
    return AttributionAdditiveSSResult{obj["is_attributed"].asInt()};
  }
};

} // namespace pcf2_he
