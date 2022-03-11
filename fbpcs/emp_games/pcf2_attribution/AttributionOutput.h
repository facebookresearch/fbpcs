/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>

#include "fbpcs/emp_games/common/Constants.h"
#include "fbpcs/emp_games/common/Debug.h"
#include "fbpcs/emp_games/pcf2_attribution/Constants.h"

namespace pcf2_attribution {

/**
 * Store plaintext attribution result
 */
struct OutputMetricDefault {
  bool is_attributed;

  folly::dynamic toDynamic() const {
    return folly::dynamic::object("is_attributed", is_attributed);
  }

  static OutputMetricDefault fromDynamic(const folly::dynamic& obj) {
    OutputMetricDefault out = OutputMetricDefault{};
    out.is_attributed = obj["is_attributed"].asBool();
    return out;
  }
};

/**
 * Store map from uid to vector of attribution results
 */
struct AttributionDefaultFmt {
  std::unordered_map<int64_t, std::vector<OutputMetricDefault>> idToMetrics;

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

template <int schedulerId, bool usingBatch = true>
class AttributionOutput {
 public:
  AttributionOutput(
      const std::vector<int64_t>& uids,
      const std::vector<SecBitT<schedulerId, usingBatch>>& attributions)
      : uids_{uids}, attributions_{attributions} {}

  /**
   * Reveal attribution result as XOR secret shares
   */
  AttributionResult reveal() {
    AttributionDefaultFmt out;

    std::vector<std::vector<bool>> revealedAttribution;
    for (auto& attributionArray : attributions_) {
      if constexpr (usingBatch) {
        IF_OMNISCIENT_MODE {
          revealedAttribution.push_back(
              attributionArray.openToParty(common::PUBLISHER).getValue());
        }
        else {
          revealedAttribution.push_back(
              attributionArray.extractBit().getValue());
        }
      } else {
        std::vector<bool> revealedAttributionArray;
        for (auto& attribution : attributionArray) {
          IF_OMNISCIENT_MODE {
            revealedAttributionArray.push_back(
                attribution.openToParty(common::PUBLISHER).getValue());
          }
          else {
            revealedAttributionArray.push_back(
                attribution.extractBit().getValue());
          }
        }
        revealedAttribution.push_back(std::move(revealedAttributionArray));
      }
    }

    // Count number of attributions for debugging
    uint32_t attributionCountOmniscient = 0;

    for (size_t i = 0; i < uids_.size(); ++i) {
      std::vector<OutputMetricDefault> revealedMetric;
      if constexpr (usingBatch) {
        for (size_t j = 0; j < revealedAttribution.size(); ++j) {
          OutputMetricDefault outputMetric{revealedAttribution.at(j).at(i)};
          revealedMetric.emplace_back(outputMetric);
          IF_OMNISCIENT_MODE {
            if (revealedAttribution.at(j).at(i)) {
              attributionCountOmniscient++;
            }
          }
        }
      } else {
        // revealedAttribution for non-batch is related to batch by transposing
        for (size_t j = 0; j < revealedAttribution.at(i).size(); ++j) {
          OutputMetricDefault outputMetric{revealedAttribution.at(i).at(j)};
          revealedMetric.emplace_back(outputMetric);
          IF_OMNISCIENT_MODE {
            if (revealedAttribution.at(i).at(j)) {
              attributionCountOmniscient++;
            }
          }
        }
      }
      out.idToMetrics.emplace(uids_.at(i), revealedMetric);
    }

    OMNISCIENT_ONLY_XLOGF(
        DBG, "Attribution count: {}", attributionCountOmniscient);

    return out.toDynamic();
  }

 private:
  std::vector<int64_t> uids_;
  std::vector<SecBitT<schedulerId, usingBatch>> attributions_;
};

} // namespace pcf2_attribution
