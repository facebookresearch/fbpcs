/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionOutput.h"
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>
#include "folly/dynamic.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Constants.h"

namespace aggregation::private_attribution {

using PrivateAttDefaultMap = std::vector<PrivateOutputMetricDefault>;
using PrivateAttErrorMap = std::vector<std::unordered_map<int64_t,PrivateOutputMetricWithError>>;

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

struct AttributionErrorFmt {
  std::unordered_map<int64_t, std::vector<std::unordered_map<int64_t,OutputMetricWithError>>> idToMetrics;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();

    for (const auto& [k1, v1] : idToMetrics) {
      auto uid = std::to_string(k1);
      folly::dynamic metricList = folly::dynamic::array();
      for (const auto& metric : v1) {
        for (const auto& item : metric) {
          folly::dynamic sub_res = folly::dynamic::object();
          sub_res.insert(std::to_string(item.first), item.second.toDynamic());
          metricList.push_back(sub_res);
        }
      }
      res.insert(uid, metricList);
    }
    return res;
  }
};


class AttributionWithError : public AttributionOutput {
 public:
  explicit AttributionWithError(
      AttributionRule attributionRule,
      const std::vector<int64_t>& uids,
      const std::vector<std::vector<PrivateTouchpoint>>& touchpoints,
      const fbpcf::Visibility& outputVisibility)
      : AttributionOutput(attributionRule, outputVisibility) {
    CHECK_EQ(uids.size(), touchpoints.size())
        << "uid array and touchpoint array must be equal size";

    for (auto i = 0; i < uids.size(); i++) {
      idToMetrics_.emplace(uids[i], PrivateAttErrorMap{});
    }
  }

  virtual void addAttribution(const PrivateAttribution& attribution) override {
    const emp::Bit true_bit{true, emp::PUBLIC};
    const emp::Bit false_bit{false, emp::PUBLIC};

    PrivateAttErrorMap& metrics = idToMetrics_[attribution.uid];
    auto is_att = emp::If(attribution.hasAttributedTouchpoint, true_bit, false_bit);
    auto tp_ts = emp::Integer(INT_SIZE, attribution.tp.ts.reveal<int64_t>(emp::PUBLIC));
    auto conv_ts = emp::Integer(INT_SIZE, attribution.conv.ts.reveal<int64_t>(emp::PUBLIC));

    int64_t code = -1; // if eventually -1, that means undetermined cause
    if (is_att.reveal<bool>(emp::PUBLIC)){
      code = 0;  // all okay
    } else if (!attribution.tp.isClick.reveal<bool>()){
      code = 1;  // is not click
    } else if(attribution.tp.ts.reveal<int64_t>() < 1){
      code = 2;  // invalid touchpoint
    } else if (attribution.tp.ts.reveal<int64_t>() >= attribution.conv.ts.reveal<int64_t>()) {
      code = 3;  // touchpoint timestamp > conversion timestamp
    } else if ((attribution.conv.ts.reveal<int64_t>() - attribution.tp.ts.reveal<int64_t>()) >= attributionRule_.window_in_sec) {
      code = 4;  // out of attribution window
    } else if ((attribution.conv.ts.reveal<int64_t>() - attribution.tp.ts.reveal<int64_t>()) < attributionRule_.window_in_sec) {
      code = 5;  // valid attribution but not last click
    }

    auto error_code = emp::Integer(INT_SIZE, code);

    XLOGF(
      DBG,
      "Revealing is_att={}, tp_ts={}, conv_ts={}, code={}",
      is_att.reveal<bool>(emp::PUBLIC),
      tp_ts.reveal<int64_t>(emp::PUBLIC),
      conv_ts.reveal<int64_t>(emp::PUBLIC),
      error_code.reveal<int64_t>(emp::PUBLIC)
    );

    PrivateOutputMetricWithError metric{
      is_att, tp_ts, conv_ts, error_code
    };

    int top_index = metrics.size();
    std::unordered_map<int64_t,PrivateOutputMetricWithError> output_metric;
    output_metric.emplace(top_index, metric);
    metrics.push_back(output_metric);
  }

  virtual AttributionResult reveal() const override {
    AttributionErrorFmt out;

    for (const auto& [uid, privateIdToMetric] : idToMetrics_) {
      XLOGF(
          DBG,
          "Revealing attribution metrics for rule={} uid={}",
          attributionRule_.name,
          uid);

      std::vector<std::unordered_map<int64_t, OutputMetricWithError>> revealedMetric;
      for (const auto& seq_metric : privateIdToMetric) {
        for (const auto& item : seq_metric){
          std::unordered_map<int64_t, OutputMetricWithError> aa;
          aa.insert(std::make_pair(item.first, item.second.reveal(fbpcf::Visibility::Public)));
          revealedMetric.push_back(aa);
        }
      }
      out.idToMetrics.emplace(uid, revealedMetric);
    }
    return out.toDynamic();
  }

 private:
  std::unordered_map<int64_t, PrivateAttErrorMap> idToMetrics_;
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
    AttributionFormat{
        /* id */ 2,
        /* name */ "debug",
        /* newAttributor */
        [](AttributionRule rule,
           AttributionContext ctx,
           fbpcf::Visibility outputVisibility)
            -> std::unique_ptr<AttributionOutput> {
          return std::make_unique<AttributionWithError>(
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
