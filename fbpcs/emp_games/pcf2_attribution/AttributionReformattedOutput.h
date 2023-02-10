/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/dynamic.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Debug.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"

namespace pcf2_attribution {

/**
 * Store plaintext attribution result
 */
struct OutputMetricReformatted {
  uint16_t ad_id;
  uint64_t conv_value;
  bool is_attributed;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();
    res.insert("ad_id", ad_id);
    res.insert("conv_value", conv_value);
    res.insert("is_attributed", is_attributed);
    return res;
  }
  static OutputMetricReformatted fromDynamic(const folly::dynamic& obj) {
    OutputMetricReformatted out = OutputMetricReformatted{};
    out.is_attributed = obj["is_attributed"].asBool();
    out.ad_id = obj["ad_id"].asInt();
    out.conv_value = obj["conv_value"].asInt();
    return out;
  }
};

/**
 * Store map from uid to vector of attribution results
 */
struct AttributionReformattedFmt {
  std::unordered_map<int64_t, std::vector<OutputMetricReformatted>> idToMetrics;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();

    for (const auto& [k1, v1] : idToMetrics) {
      auto uid = std::to_string(k1);
      folly::dynamic metricList = folly::dynamic::array();
      for (const auto& metric : v1) {
        metricList.push_back(metric.toDynamic());
      }
      res.insert(uid, metricList);
    }
    return res;
  }
};

using AttributionResult = folly::dynamic;

template <int schedulerId>
struct AttributionReformattedOutputFmt {
  SecAdId<schedulerId> ad_id;
  SecConvValue<schedulerId> conv_value;
  SecBit<schedulerId> is_attributed;
};

template <int schedulerId>
using AttributionReformattedOutputFmtT =
    AttributionReformattedOutputFmt<schedulerId>;

template <int schedulerId = true>
class AttributionReformattedOutput {
 public:
  AttributionReformattedOutput(
      const std::vector<int64_t>& uids,
      const std::vector<AttributionReformattedOutputFmtT<schedulerId>>&
          attributionStruct)
      : uids_{uids}, attributionStruct_{attributionStruct} {}

  /**
   * Reveal attribution result as XOR secret shares
   */
  AttributionResult reveal() {
    AttributionReformattedFmt out;

    std::vector<std::vector<uint64_t>> revealedAdId;
    std::vector<std::vector<uint64_t>> revealedConvValue;
    std::vector<std::vector<bool>> revealedAttribution;
    for (const auto& attributionStructArray : attributionStruct_) {
      IF_OMNISCIENT_MODE {
        /* reveal ad_ids */
        revealedAdId.push_back(
            attributionStructArray.ad_id.openToParty(common::PUBLISHER)
                .getValue());
        /* reveal conv_value */
        revealedConvValue.push_back(
            attributionStructArray.conv_value.openToParty(common::PUBLISHER)
                .getValue());
        /* reveal is_attributed */
        revealedAttribution.push_back(
            attributionStructArray.is_attributed.openToParty(common::PUBLISHER)
                .getValue());
      }
      else {
        revealedAdId.push_back(
            attributionStructArray.ad_id.extractIntShare().getValue());
        revealedConvValue.push_back(
            attributionStructArray.conv_value.extractIntShare().getValue());
        revealedAttribution.push_back(
            attributionStructArray.is_attributed.extractBit().getValue());
      }
    }

    // Count number of attributions for debugging
    uint32_t adIdCountOmniscient = 0;
    uint32_t convValueSumOmniscient = 0;
    uint32_t attributionCountOmniscient = 0;

    for (size_t i = 0; i < uids_.size(); ++i) {
      std::vector<OutputMetricReformatted> revealedMetric;

      for (size_t j = 0; j < revealedAdId.size(); ++j) {
        OutputMetricReformatted outputMetric{
            static_cast<uint16_t>(revealedAdId.at(j).at(i)),
            revealedConvValue.at(j).at(i),
            revealedAttribution.at(j).at(i)};
        revealedMetric.emplace_back(outputMetric);
        IF_OMNISCIENT_MODE {
          if (revealedAdId.at(j).at(i)) {
            adIdCountOmniscient++;
          }
          convValueSumOmniscient += revealedConvValue.at(j).at(i);
          if (revealedAttribution.at(j).at(i)) {
            attributionCountOmniscient++;
          }
        }
      }

      out.idToMetrics.emplace(uids_.at(i), revealedMetric);
    }

    OMNISCIENT_ONLY_XLOGF(DBG, "Ad_id count: {}", adIdCountOmniscient);
    OMNISCIENT_ONLY_XLOGF(
        DBG, "Conversion_values sum: {}", convValueSumOmniscient);
    OMNISCIENT_ONLY_XLOGF(
        DBG, "Attribution count: {}", attributionCountOmniscient);

    return out.toDynamic();
  }

 private:
  std::vector<int64_t> uids_;
  std::vector<AttributionReformattedOutputFmtT<schedulerId>> attributionStruct_;
};
} // namespace pcf2_attribution
