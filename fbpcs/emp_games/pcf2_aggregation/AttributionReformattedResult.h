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

struct AttributionReformattedResult {
  uint64_t adId;
  uint64_t convValue;
  bool isAttributed;

  static AttributionReformattedResult fromDynamic(const folly::dynamic& obj) {
    AttributionReformattedResult out = AttributionReformattedResult{};

    out.adId = obj["ad_id"].asInt();
    out.convValue = obj["conv_value"].asInt();
    out.isAttributed = obj["is_attributed"].asBool();
    return out;
  }
};

template <int schedulerId>
struct PrivateAttributionReformattedResult {
  explicit PrivateAttributionReformattedResult(
      const AttributionReformattedResult& attributionReformattedResult) {
    typename SecBit<schedulerId>::ExtractedBit extractedAttribution(
        attributionReformattedResult.isAttributed);
    this->isAttributed = SecBit<schedulerId>(std::move(extractedAttribution));

    typename SecAdId<schedulerId>::ExtractedInt extractedAdId(
        attributionReformattedResult.adId);
    this->adId = SecAdId<schedulerId>(std::move(extractedAdId));

    typename SecConvValue<schedulerId>::ExtractedInt extractedConvValue(
        attributionReformattedResult.convValue);
    this->convValue = SecConvValue<schedulerId>(std::move(extractedConvValue));
  }

  SecBit<schedulerId> isAttributed;
  SecAdId<schedulerId> adId;
  SecConvValue<schedulerId> convValue;
};

} // namespace pcf2_aggregation
