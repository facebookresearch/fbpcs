/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "Aggregator.h"
#include <algorithm>
#include <iterator>
#include <string>
#include <utility>
#include "folly/dynamic.h"

namespace measurement::private_attribution {

using PrivateConvMap = std::vector<std::pair<emp::Integer, PrivateConvMetrics>>;
using PrivateAemConvMap =
    std::vector<std::pair<emp::Integer, PrivateAemConvMetric>>;
using PrivatePcmConvMap =
    std::vector<std::pair<emp::Integer, std::vector<PrivatePcmMetrics>>>;

namespace {

struct MeasurementAggregation {
  // ad_id => metrics
  std::unordered_map<int64_t, ConvMetrics> metrics;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();

    for (const auto& [k, v] : metrics) {
      auto key = std::to_string(k);
      auto val = v.toDynamic();
      res.insert(key, val);
    }

    return res;
  }
};

class MeasurementAggregator : public Aggregator {
 public:
  explicit MeasurementAggregator(
      AttributionRule attributionRule,
      const std::vector<int64_t>& validAdIds,
      const fbpcf::Visibility& outputVisibility)
      : Aggregator{attributionRule, outputVisibility} {
    for (auto adId : validAdIds) {
      _adIdToMetrics.push_back(std::make_pair(
          emp::Integer{INT_SIZE, adId, emp::PUBLIC}, PrivateConvMetrics{}));
    }
  }

  virtual void addAttribution(const PrivateAttribution& attribution) override {
    for (auto& [adId, metrics] : _adIdToMetrics) {
      const emp::Integer zero{INT_SIZE, 0, emp::PUBLIC};
      const emp::Integer one{INT_SIZE, 1, emp::PUBLIC};

      const auto adIdMatches =
          attribution.hasAttributedTouchpoint & adId.equal(attribution.tp.adId);
      // emp::If(condition, true_case, false_case)
      const auto convsDelta = emp::If(adIdMatches, one, zero);
      const auto salesDelta =
          emp::If(adIdMatches, attribution.conv.conv_value, zero);

      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "Aggregating for adId={}, metrics=[convs={}, sales={}], thisAdId={}, matches={}, convsDelta={}, salesDelta={}",
          adId.reveal<int64_t>(),
          metrics.reveal(outputVisibility_).convs,
          metrics.reveal(outputVisibility_).sales,
          attribution.tp.adId.reveal<int64_t>(),
          adIdMatches.reveal<bool>(),
          convsDelta.reveal<int64_t>(),
          salesDelta.reveal<int64_t>());

      metrics.convs = metrics.convs + convsDelta;
      metrics.sales = metrics.sales + salesDelta;
    }
  }

  virtual Aggregation reveal() const override {
    MeasurementAggregation out;

    for (auto& [adId, metrics] : _adIdToMetrics) {
      const auto rAdId = adId.reveal<int64_t>();
      XLOGF(
          DBG,
          "Revealing measurement metrics for {} adId={}",
          attributionRule_.name,
          rAdId);
      const auto rMetrics = metrics.reveal(outputVisibility_);
      out.metrics[rAdId] = rMetrics;
    }

    return out.toDynamic();
  }

 private:
  PrivateConvMap _adIdToMetrics;
};

struct DeliveryAggregation {
  // uid => imp_id => metrics
  std::unordered_map<int64_t, std::unordered_map<int64_t, ConvMetrics>>
      uidToImpToMetrics;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();

    for (const auto& [k1, v1] : uidToImpToMetrics) {
      auto uid = std::to_string(k1);

      folly::dynamic impToMetrics = folly::dynamic::object();
      for (const auto& [k2, v2] : v1) {
        auto impId = std::to_string(k2);
        auto metrics = v2.toDynamic();
        impToMetrics.insert(impId, metrics);
      }

      res.insert(uid, impToMetrics);
    }

    return res;
  }
};

class DeliveryAggregator : public Aggregator {
 public:
  explicit DeliveryAggregator(
      AttributionRule attributionRule,
      const std::vector<int64_t>& uids,
      const std::vector<std::vector<PrivateTouchpoint>>& touchpoints,
      const fbpcf::Visibility& outputVisibility)
      : Aggregator(attributionRule, outputVisibility) {
    CHECK_EQ(uids.size(), touchpoints.size())
        << "uid array and touchpoint array must be equal size";

    for (std::vector<int64_t>::size_type i = 0; i < uids.size(); i++) {
      auto uid = uids[i];
      auto& tps = touchpoints[i];

      PrivateConvMap impToConvs;
      for (auto tp : tps) {
        auto impId = tp.id;
        impToConvs.push_back(std::make_pair(impId, PrivateConvMetrics{}));
      }

      uidToImpToMetrics_.emplace(std::make_pair(uid, impToConvs));
    }
  }

  virtual void addAttribution(const PrivateAttribution& attribution) override {
    PrivateConvMap& map = uidToImpToMetrics_[attribution.uid];
    for (auto& [impId, metrics] : map) {
      const emp::Integer zero{INT_SIZE, 0, emp::PUBLIC};
      const emp::Integer one{INT_SIZE, 1, emp::PUBLIC};

      const auto impIdMatches =
          attribution.hasAttributedTouchpoint & impId.equal(attribution.tp.id);
      const auto convsDelta = zero.select(impIdMatches, one);
      const auto salesDelta =
          zero.select(impIdMatches, attribution.conv.conv_value);

      OMNISCIENT_ONLY_XLOGF(
          DBG,
          "Aggregating uid={} imp={}, metrics=[convs={}, sales={}], thisImpId={}, matches={}, convsDelta={}, salesDelta={}",
          attribution.uid,
          impId.reveal<int64_t>(),
          metrics.reveal(outputVisibility_).convs,
          metrics.reveal(outputVisibility_).sales,
          attribution.tp.id.reveal<int64_t>(),
          impIdMatches.reveal<bool>(),
          convsDelta.reveal<int64_t>(),
          salesDelta.reveal<int64_t>());

      metrics.convs = metrics.convs + convsDelta;
      metrics.sales = metrics.sales + salesDelta;
    }
  }
  virtual Aggregation reveal() const override {
    DeliveryAggregation out;

    for (const auto& [uid, privateImpToMetrics] : uidToImpToMetrics_) {
      XLOGF(
          DBG,
          "Revealing delivery metrics for rule={} uid={}",
          attributionRule_.name,
          uid);

      std::unordered_map<int64_t, ConvMetrics> impToMetrics;
      for (const auto& [privateImpId, privateMetrics] : privateImpToMetrics) {
        // If we're in omniscient mode, reveal the result publicly for easier
        // debugging
        IF_OMNISCIENT_MODE {
          const auto impId = privateImpId.reveal<int64_t>(emp::PUBLIC);
          const auto metrics = privateMetrics.reveal(fbpcf::Visibility::Public);
          impToMetrics[impId] = metrics;
        }
        else {
          const auto impId = privateImpId.reveal<int64_t>(emp::XOR);
          const auto metrics = privateMetrics.reveal(fbpcf::Visibility::Xor);
          impToMetrics[impId] = metrics;
        }
      }

      out.uidToImpToMetrics[uid] = impToMetrics;
    }

    return out.toDynamic();
  }

 private:
  std::unordered_map<int64_t, PrivateConvMap> uidToImpToMetrics_;
};

struct AttributionAggregation {
  std::vector<
      std::pair<int64_t, std::vector<std::pair<int64_t, AemConvMetric>>>>
      uidToImpToMetrics;

  folly::dynamic toDynamic() const {
    folly::dynamic res = folly::dynamic::object();

    for (const auto& [k1, v1] : uidToImpToMetrics) {
      auto uid = std::to_string(k1);

      folly::dynamic impToMetrics = folly::dynamic::object();
      for (const auto& [k2, v2] : v1) {
        auto impId = std::to_string(k2);
        auto metrics = v2.toDynamic();
        impToMetrics.insert(impId, metrics);
      }
      res.insert(uid, impToMetrics);
    }
    return res;
  }
};

// TODO: Try to split aem into a separate file
class AttributionAggregator : public Aggregator {
 public:
  explicit AttributionAggregator(
      AttributionRule attributionRule,
      const std::vector<int64_t>& uids,
      const std::vector<std::vector<PrivateTouchpoint>>& touchpoints,
      const fbpcf::Visibility& outputVisibility)
      : Aggregator(attributionRule, outputVisibility) {
    CHECK_EQ(uids.size(), touchpoints.size())
        << "uid array and touchpoint array must be equal size";

    for (std::vector<int64_t>::size_type i = 0; i < uids.size(); i++) {
      auto uid = uids[i];
      auto& tps = touchpoints[i];

      PrivateAemConvMap impToAemConvs;
      for (auto tp : tps) {
        auto impId = tp.id;
        impToAemConvs.push_back(std::make_pair(impId, PrivateAemConvMetric{}));
      }
      uidToImpToMetrics_.emplace_back(std::make_pair(uid, impToAemConvs));
    }
  }

  virtual void addAttribution(const PrivateAttribution& attribution) override {
    PrivateAemConvMap* map = nullptr;
    for (auto& [uid, imgIdToMetrics] : uidToImpToMetrics_) {
      if (attribution.uid == uid) {
        map = &imgIdToMetrics;
      }
    }

    CHECK_NOTNULL(map);
    for (auto& [impId, metrics] : *map) {
      const emp::Integer dummy{INT_SIZE, -1, emp::PUBLIC};
      const emp::Bit true_bit{true, emp::PUBLIC};
      const emp::Bit false_bit{false, emp::PUBLIC};

      const auto adIdMatches =
          attribution.hasAttributedTouchpoint & impId.equal(attribution.tp.id);

      const auto conversion_bits =
          emp::If(adIdMatches, attribution.conv.conv_metadata, dummy);
      const auto is_attributed = emp::If(adIdMatches, true_bit, false_bit);

      metrics.campaign_bits = emp::If(
          adIdMatches, attribution.tp.campaignMetadata, metrics.campaign_bits);
      ;
      metrics.conversion_bits.push_back(conversion_bits);
      metrics.is_attributed.push_back(is_attributed);
    }
  }
  virtual Aggregation reveal() const override {
    AttributionAggregation out;

    for (const auto& [uid, privateImpToMetrics] : uidToImpToMetrics_) {
      XLOGF(
          DBG,
          "Revealing AEM metrics for rule={} uid={}",
          attributionRule_.name,
          uid);

      std::vector<std::pair<int64_t, AemConvMetric>> impToMetrics;
      for (const auto& [privateImpId, privateMetrics] : privateImpToMetrics) {
        // If we're in omniscient mode, reveal the result publicly for easier
        // debugging
        IF_OMNISCIENT_MODE {
          const auto impId = privateImpId.reveal<int64_t>(emp::PUBLIC);
          const auto metrics = privateMetrics.reveal(fbpcf::Visibility::Public);
          impToMetrics.emplace_back(std::make_pair(impId, metrics));
        }
        else {
          const auto impId = privateImpId.reveal<int64_t>(emp::XOR);
          const auto metrics = privateMetrics.reveal(fbpcf::Visibility::Xor);
          impToMetrics.emplace_back(std::make_pair(impId, metrics));
        }
      }
      out.uidToImpToMetrics.emplace_back(std::make_pair(uid, impToMetrics));
    }
    return out.toDynamic();
  }

 private:
  std::vector<std::pair<int64_t, PrivateAemConvMap>> uidToImpToMetrics_;
};

struct PcmAggregation {
  std::unordered_map<std::pair<int64_t, int64_t>, int64_t>
      campaignToConversionBitsCount;

  folly::dynamic toDynamic() const {
    folly::dynamic resDict = folly::dynamic::object();

    for (const auto& [k, v] : campaignToConversionBitsCount) {
      folly::dynamic res = folly::dynamic::object();

      res = folly::dynamic::object("campaign_bits", k.first)(
          "conversion_bits", k.second)("count", v);

      resDict.insert(
          std::to_string(k.first) + ":" + std::to_string(k.second), res);
    }
    return resDict;
  }
};

class PcmAggregator : public Aggregator {
 public:
  explicit PcmAggregator(
      AttributionRule attributionRule,
      const std::vector<int64_t>& uids,
      const std::vector<std::vector<PrivateTouchpoint>>& touchpoints,
      const fbpcf::Visibility& outputVisibility)
      : Aggregator(attributionRule, outputVisibility) {
    CHECK_EQ(uids.size(), touchpoints.size())
        << "uid array and touchpoint array must be equal size";

    for (std::vector<int64_t>::size_type i = 0; i < uids.size(); i++) {
      auto uid = uids[i];
      auto& tps = touchpoints[i];

      PrivatePcmConvMap impToPcmConvs;
      for (auto tp : tps) {
        auto impId = tp.id;
        std::vector<PrivatePcmMetrics> metricsList;
        impToPcmConvs.push_back(std::make_pair(impId, metricsList));
      }
      uidToImpToPcmMetrics_.emplace_back(std::make_pair(uid, impToPcmConvs));
    }
  }

  virtual void addAttribution(const PrivateAttribution& attribution) override {
    PrivatePcmConvMap* map = nullptr;
    for (auto& [uid, impIdToMetrics] : uidToImpToPcmMetrics_) {
      if (attribution.uid == uid) {
        map = &impIdToMetrics;
      }
    }

    CHECK_NOTNULL(map);
    for (auto& [impId, metricsList] : *map) {
      const auto isAttributed =
          attribution.hasAttributedTouchpoint & impId.equal(attribution.tp.id);

      PrivatePcmMetrics metrics;
      metrics.campaign_bits = emp::If(
          isAttributed, attribution.tp.campaignMetadata, metrics.campaign_bits);
      metrics.conversion_bits = emp::If(
          isAttributed,
          attribution.conv.conv_metadata,
          metrics.conversion_bits);
      metricsList.push_back(metrics);
    }
  }

  virtual Aggregation reveal() const override {
    PcmAggregation out;

    for (const auto& [uid, impIdToMetrics] : uidToImpToPcmMetrics_) {
      XLOGF(
          DBG,
          "Revealing PCM aggregation results for rule={}, uid={}",
          attributionRule_.name,
          uid);

      for (const auto& [privateImpId, metricsList] : impIdToMetrics) {
        for (const auto& metrics : metricsList) {
          IF_OMNISCIENT_MODE {
            const auto campaign_bits =
                metrics.campaign_bits.reveal<int64_t>(emp::PUBLIC);
            const auto conversion_bits =
                metrics.conversion_bits.reveal<int64_t>(emp::PUBLIC);

            if ((campaign_bits != 0) && (conversion_bits != 0)) {
              const auto key = std::pair(campaign_bits, conversion_bits);
              if (out.campaignToConversionBitsCount.find(key) !=
                  out.campaignToConversionBitsCount.end()) {
                out.campaignToConversionBitsCount[key] += 1;
              } else {
                out.campaignToConversionBitsCount[key] = 1;
              }
            }
          }
          else {
            // Revealing plaintext to publisher side only
            const auto campaign_bits =
                metrics.campaign_bits.reveal<int64_t>(emp::ALICE);
            const auto conversion_bits =
                metrics.conversion_bits.reveal<int64_t>(emp::ALICE);

            // skipping over the non-attributed metrics
            if ((campaign_bits != 0) && (conversion_bits != 0)) {
              const auto key = std::pair(campaign_bits, conversion_bits);
              if (out.campaignToConversionBitsCount.find(key) !=
                  out.campaignToConversionBitsCount.end()) {
                out.campaignToConversionBitsCount[key] += 1;
              } else {
                out.campaignToConversionBitsCount[key] = 1;
              }
            }
          }
        }
      }
    }
    return out.toDynamic();
  }

 private:
  std::vector<std::pair<int64_t, PrivatePcmConvMap>> uidToImpToPcmMetrics_;
};

} // namespace

static const std::array SUPPORTED_AGGREGATION_FORMATS{
    AggregationFormat{
        /* id */ 1,
        /* name */ "measurement",
        /* newAggregator */
        [](AttributionRule rule,
           AggregationContext ctx,
           fbpcf::Visibility outputVisibility) -> std::unique_ptr<Aggregator> {
          return std::make_unique<MeasurementAggregator>(
              rule, ctx.validAdIds, outputVisibility);
        },
    },
    AggregationFormat{
        /* id */ 2,
        /* name */ "delivery",
        /* newAggregator */
        [](AttributionRule rule,
           AggregationContext ctx,
           fbpcf::Visibility outputVisibility) -> std::unique_ptr<Aggregator> {
          return std::make_unique<DeliveryAggregator>(
              rule, ctx.uids, ctx.touchpoints, outputVisibility);
        },
    },
    AggregationFormat{
        /* id */ 3,
        /* name */ "attribution",
        /* newAggregator */
        [](AttributionRule rule,
           AggregationContext ctx,
           fbpcf::Visibility outputVisibility) -> std::unique_ptr<Aggregator> {
          return std::make_unique<AttributionAggregator>(
              rule, ctx.uids, ctx.touchpoints, outputVisibility);
        },
    },
    AggregationFormat{
        /* id */ 4,
        /* name */ "pcm_ify",
        /* newAggregator */
        [](AttributionRule rule,
           AggregationContext ctx,
           fbpcf::Visibility outputVisibility) -> std::unique_ptr<Aggregator> {
          return std::make_unique<PcmAggregator>(
              rule, ctx.uids, ctx.touchpoints, outputVisibility);
        },
    },
};

AggregationFormat getAggregationFormatFromNameOrThrow(const std::string& name) {
  for (auto rule : SUPPORTED_AGGREGATION_FORMATS) {
    if (rule.name == name) {
      return rule;
    }
  }

  throw std::runtime_error("Unknown aggregation rule name: " + name);
}

AggregationFormat getAggregationFormatFromIdOrThrow(int64_t id) {
  for (auto rule : SUPPORTED_AGGREGATION_FORMATS) {
    if (rule.id == id) {
      return rule;
    }
  }

  throw std::runtime_error(fmt::format("Unknown aggregation id: {}", id));
}

} // namespace measurement::private_attribution
