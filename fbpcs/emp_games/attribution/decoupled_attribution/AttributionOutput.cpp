/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionOutput.h"
#include <algorithm>
#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>
#include "folly/dynamic.h"

namespace aggregation::private_attribution {

using PrivateAttDefaultMap = std::vector<PrivateOutputMetricDefault>;

namespace {

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

class AttributionDefault : public AttributionOutput {
 public:
  explicit AttributionDefault(
      AttributionRule attributionRule,
      const std::vector<int64_t>& uids,
      const std::vector<std::vector<PrivateTouchpoint>>& touchpoints,
      const fbpcf::Visibility& outputVisibility)
      : AttributionOutput(attributionRule, outputVisibility) {
    CHECK_EQ(uids.size(), touchpoints.size())
        << "uid array and touchpoint array must be equal size";

    for (std::vector<int64_t>::size_type i = 0; i < uids.size(); i++) {
      idToMetrics_.emplace(uids[i], PrivateAttDefaultMap{});
    }
  }

  virtual void addAttribution(const PrivateAttribution& attribution) override {
    const emp::Bit true_bit{true, emp::PUBLIC};
    const emp::Bit false_bit{false, emp::PUBLIC};

    PrivateAttDefaultMap& metrics = idToMetrics_[attribution.uid];
    PrivateOutputMetricDefault metric{
        emp::If(attribution.hasAttributedTouchpoint, true_bit, false_bit)};
    metrics.push_back(metric);
  }

  virtual AttributionResult reveal() const override {
    AttributionDefaultFmt out;

    for (const auto& [uid, privateIdToMetric] : idToMetrics_) {
      XLOGF(
          DBG,
          "Revealing attribution metrics for rule={} uid={}",
          attributionRule_.name,
          uid);

      std::vector<OutputMetricDefault> revealedMetric;
      for (const auto& metric : privateIdToMetric) {
        IF_OMNISCIENT_MODE {
          revealedMetric.emplace_back(metric.reveal(fbpcf::Visibility::Public));
        }
        else {
          revealedMetric.emplace_back(metric.reveal(fbpcf::Visibility::Xor));
        }
      }
      out.idToMetrics.emplace(uid, revealedMetric);
    }
    return out.toDynamic();
  }

 private:
  std::unordered_map<int64_t, PrivateAttDefaultMap> idToMetrics_;
};

} // namespace

static const std::array SUPPORTED_ATTRIBUTION_FORMATS{
    AttributionFormat{
        /* id */ 1,
        /* name */ "default",
        /* newAttributor */
        [](AttributionRule rule,
           AttributionContext ctx,
           fbpcf::Visibility outputVisibility)
            -> std::unique_ptr<AttributionOutput> {
          return std::make_unique<AttributionDefault>(
              rule, ctx.uids, ctx.touchpoints, outputVisibility);
        },
    },
};

AttributionFormat getAttributionFormatFromNameOrThrow(const std::string& name) {
  for (auto rule : SUPPORTED_ATTRIBUTION_FORMATS) {
    if (rule.name == name) {
      return rule;
    }
  }

  throw std::runtime_error("Unknown attribution rule name: " + name);
}

AttributionFormat getAttributionFormatFromIdOrThrow(int64_t id) {
  for (auto rule : SUPPORTED_ATTRIBUTION_FORMATS) {
    if (rule.id == id) {
      return rule;
    }
  }

  throw std::runtime_error(fmt::format("Unknown attribution id: {}", id));
}

} // namespace aggregation::private_attribution
