/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>

#include <folly/dynamic.h>
#include <cstdlib>

#include <fbpcf/mpc/EmpGame.h>
#include "AttributionRule.h"
#include "Conversion.h"
#include "Touchpoint.h"

namespace measurement::private_attribution {

struct PrivateAttribution {
  int64_t uid;
  emp::Bit hasAttributedTouchpoint;
  PrivateConversion conv;
  PrivateTouchpoint tp;

  explicit PrivateAttribution(
      int64_t _uid,
      const emp::Bit& _hasAttributedTouchpoint,
      const PrivateConversion& _conv,
      const PrivateTouchpoint& _tp)
      : uid{_uid},
        hasAttributedTouchpoint{_hasAttributedTouchpoint},
        conv{_conv},
        tp{_tp} {}
};

// TODO: Try to split aems into a separate header file
struct AemConvMetric {
  int64_t campaign_bits;
  std::vector<int64_t> conversion_bits;
  std::vector<bool> is_attributed;

  folly::dynamic toStringConvertionBits() const {
    folly::dynamic res = folly::dynamic::array();
    for (auto& it : conversion_bits) {
      res.push_back(it);
    }
    return res;
  }
  folly::dynamic toStringIsAttributed() const {
    folly::dynamic res = folly::dynamic::array();
    for (auto it : is_attributed) {
      res.push_back(it);
    }
    return res;
  }
  folly::dynamic toDynamic() const {
    return folly::dynamic::object("campaign_bit", campaign_bits)(
        "conversion_bit", toStringConvertionBits())(
        "is_attributed", toStringIsAttributed());
  }
  static AemConvMetric fromDynamic(const folly::dynamic& obj) {
    AemConvMetric out = AemConvMetric{};
    out.campaign_bits = obj["campaign_bit"].asInt();
    for (auto& it : obj["conversion_bit"]) {
      out.conversion_bits.push_back(it.asInt());
    }
    for (auto& it : obj["is_attributed"]) {
      out.is_attributed.push_back(it.asBool());
    }
    return out;
  }
};

struct PrivateAemConvMetric {
  emp::Integer campaign_bits{INT_SIZE, 0, emp::PUBLIC};
  std::vector<emp::Integer> conversion_bits;
  std::vector<emp::Bit> is_attributed;

  AemConvMetric reveal(fbpcf::Visibility outputVisibility) const {
    int party =
        outputVisibility == fbpcf::Visibility::Xor ? emp::XOR : emp::PUBLIC;

    std::vector<int64_t> conv_bits;
    for (auto& it : conversion_bits) {
      conv_bits.push_back(it.reveal<int64_t>(party));
    }
    std::vector<bool> is_att;
    for (auto it : is_attributed) {
      is_att.push_back(it.reveal<bool>(party));
    }
    return AemConvMetric{
        campaign_bits.reveal<int64_t>(party), conv_bits, is_att};
  }
};

struct PcmMetrics {
  int64_t campaign_bits;
  int64_t conversion_bits;
  int64_t count;

  folly::dynamic toDynamic() const {
    return folly::dynamic::object("campaign_bits", campaign_bits)(
        "conversion_bits", conversion_bits)("count", count);
  }

  static PcmMetrics fromDynamic(const folly::dynamic& obj) {
    PcmMetrics out = PcmMetrics{};
    out.campaign_bits = obj["campaign_bits"].asInt();
    out.conversion_bits = obj["conversion_bits"].asInt();
    out.count = obj["count"].asInt();
    return out;
  }
};

struct PrivatePcmMetrics {
  emp::Integer campaign_bits{INT_SIZE, 0, emp::PUBLIC};
  emp::Integer conversion_bits{INT_SIZE, 0, emp::PUBLIC};
  emp::Integer count{INT_SIZE, 0, emp::PUBLIC};

  PcmMetrics reveal(fbpcf::Visibility outputVisibility) const {
    int32_t party = static_cast<int32_t>(outputVisibility);

    return PcmMetrics{
        campaign_bits.reveal<int64_t>(party),
        conversion_bits.reveal<int64_t>(party),
        count.reveal<int64_t>(party)};
  }

  static PrivatePcmMetrics fromDynamic(const folly::dynamic& obj, int party) {
    PrivatePcmMetrics out;
    out.campaign_bits =
        emp::Integer(INT_SIZE, obj["campaign_bits"].asInt(), party);
    out.conversion_bits =
        emp::Integer(INT_SIZE, obj["conversion_bits"].asInt(), party);
    out.count = emp::Integer(INT_SIZE, obj["count"].asInt(), party);
    return out;
  }
};

struct ConvMetrics {
  int64_t convs;
  int64_t sales;

  folly::dynamic toDynamic() const {
    return folly::dynamic::object("convs", convs)("sales", sales);
  }

  static ConvMetrics fromDynamic(const folly::dynamic& obj) {
    ConvMetrics out = ConvMetrics{};
    out.convs = obj["convs"].asInt();
    out.sales = obj["sales"].asInt();
    return out;
  }
};

struct PrivateConvMetrics {
  emp::Integer convs{INT_SIZE, 0, emp::PUBLIC};
  emp::Integer sales{INT_SIZE, 0, emp::PUBLIC};

  ConvMetrics reveal(fbpcf::Visibility outputVisibility) const {
    int32_t party = static_cast<int32_t>(outputVisibility);

    return ConvMetrics{
        convs.reveal<int64_t>(party), sales.reveal<int64_t>(party)};
  }

  static PrivateConvMetrics fromDynamic(const folly::dynamic& obj, int party) {
    PrivateConvMetrics out;
    out.convs = emp::Integer(INT_SIZE, obj["convs"].asInt(), party);
    out.sales = emp::Integer(INT_SIZE, obj["sales"].asInt(), party);
    return out;
  }

  PrivateConvMetrics operator^(const PrivateConvMetrics& other) const noexcept {
    PrivateConvMetrics out;
    out.convs = convs ^ other.convs;
    out.sales = sales ^ other.sales;
    return out;
  }

  PrivateConvMetrics operator+(const PrivateConvMetrics& other) const noexcept {
    PrivateConvMetrics out;
    out.convs = convs + other.convs;
    out.sales = sales + other.sales;
    return out;
  }

  static PrivateConvMetrics xoredFromDynamic(folly::dynamic& m) {
    PrivateConvMetrics aliceCM = PrivateConvMetrics::fromDynamic(m, emp::ALICE);
    PrivateConvMetrics bobCM = PrivateConvMetrics::fromDynamic(m, emp::BOB);
    return aliceCM ^ bobCM;
  }
};

using Aggregation = folly::dynamic;

class Aggregator {
 public:
  explicit Aggregator(
      AttributionRule attributionRule,
      const fbpcf::Visibility& outputVisibility)
      : attributionRule_{attributionRule},
        outputVisibility_{outputVisibility} {}

  virtual ~Aggregator() {}

  virtual void addAttribution(const PrivateAttribution& attribution) = 0;
  virtual Aggregation reveal() const = 0;

 protected:
  AttributionRule attributionRule_;
  const fbpcf::Visibility outputVisibility_;
};

struct AggregationContext {
  const std::vector<int64_t>& validAdIds;
  const std::vector<int64_t>& uids;
  const std::vector<std::vector<PrivateTouchpoint>>& touchpoints;
};

struct AggregationFormat {
  // Integer that should uniquely identify this aggregation format. Used
  // to synchronize between the publisher and partner
  int64_t id;
  // Human readable name for the this aggregation format. The publisher will
  // pass in a list of names, and the output json will be keyed by this name
  std::string name;
  // Should return a new aggregator for this aggregation format. The aggregator
  // should use the given attribution rule and aggregation context.
  std::function<std::unique_ptr<
      Aggregator>(AttributionRule, AggregationContext, fbpcf::Visibility)>
      newAggregator;
};

AggregationFormat getAggregationFormatFromNameOrThrow(const std::string& name);
AggregationFormat getAggregationFormatFromIdOrThrow(int64_t id);

} // namespace measurement::private_attribution
