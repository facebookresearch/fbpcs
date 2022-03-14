/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>

#include "fbpcs/emp_games/pcf2_aggregation/Constants.h"

namespace pcf2_aggregation {

struct AttributionResult {
  const bool isAttributed;

  static AttributionResult fromDynamic(const folly::dynamic& obj) {
    return AttributionResult{obj["is_attributed"].asBool()};
  }
};

template <int schedulerId>
struct PrivateAttributionResult {
  explicit PrivateAttributionResult(
      const AttributionResult& attributionResult) {
    typename SecBit<schedulerId>::ExtractedBit extractedAttribution(
        attributionResult.isAttributed);
    this->isAttributed = SecBit<schedulerId>(std::move(extractedAttribution));
  }

  SecBit<schedulerId> isAttributed;
};

} // namespace pcf2_aggregation
