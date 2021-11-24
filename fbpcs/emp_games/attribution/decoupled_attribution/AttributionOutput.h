/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>

#include <folly/dynamic.h>
#include <cstdlib>

#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionRule.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Conversion.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Touchpoint.h"
#include <fbpcf/mpc/EmpGame.h>

namespace aggregation::private_attribution {

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


struct OutputMetricWithError {
  bool is_attributed;
  int64_t tp_ts;
  int64_t conv_ts;
  int64_t error_code;

  folly::dynamic toDynamic() const {
    return folly::dynamic::object
        ("is_attributed", is_attributed)
        ("tp_ts", tp_ts)
        ("conv_ts", conv_ts)
        ("error_code", error_code);
  }

  static OutputMetricWithError fromDynamic(const folly::dynamic& obj) {
    OutputMetricWithError out = OutputMetricWithError{};
    out.is_attributed = obj["is_attributed"].asBool();
    out.tp_ts = obj["tp_ts"].asInt();
    out.conv_ts = obj["conv_ts"].asInt();
    out.error_code = obj["error_code"].asInt();
    return out;
  }
};


struct OutputMetricDefault {
  bool is_attributed;

  folly::dynamic toDynamic() const {
    return folly::dynamic::object(
        "is_attributed", is_attributed);
  }

  static OutputMetricDefault fromDynamic(const folly::dynamic& obj) {
    OutputMetricDefault out = OutputMetricDefault{};
    out.is_attributed = obj["is_attributed"].asBool();
    return out;
  }
};


struct PrivateOutputMetricDefault {
  emp::Bit is_attributed{false, emp::PUBLIC};

  OutputMetricDefault reveal(fbpcf::Visibility outputVisibility) const {
    int party =
        outputVisibility == fbpcf::Visibility::Xor ? emp::XOR : emp::PUBLIC;

    return OutputMetricDefault{
          is_attributed.reveal<bool>(party)};
  }
};


struct PrivateOutputMetricWithError {
  emp::Bit is_attributed{false, emp::PUBLIC};
  emp::Integer tp_ts;
  emp::Integer conv_ts;
  emp::Integer error_code;

  OutputMetricWithError reveal(fbpcf::Visibility outputVisibility) const {
    int party =
        outputVisibility == fbpcf::Visibility::Xor ? emp::XOR : emp::PUBLIC;

    return OutputMetricWithError{
          is_attributed.reveal<bool>(party),
          tp_ts.reveal<int64_t>(party),
          conv_ts.reveal<int64_t>(party),
          error_code.reveal<int64_t>(party)};
  }
};


using AttributionResult = folly::dynamic;

class AttributionOutput {
 public:
  explicit AttributionOutput(
      AttributionRule attributionRule,
      const fbpcf::Visibility& outputVisibility)
      : attributionRule_{attributionRule},
        outputVisibility_{outputVisibility} {}

  virtual ~AttributionOutput() {}

  virtual void addAttribution(const PrivateAttribution& attribution) = 0;
  virtual AttributionResult reveal() const = 0;

 protected:
  AttributionRule attributionRule_;
  const fbpcf::Visibility outputVisibility_;
};

struct AttributionContext {
  const std::vector<int64_t>& uids;
  const std::vector<std::vector<PrivateTouchpoint>>& touchpoints;
};

struct AttributionFormat {
  // Integer that should uniquely identify this attribution format. Used
  // to synchronize between the publisher and partner
  int64_t id;
  // Human readable name for the this attribution format. The publisher will
  // pass in a list of names, and the output json will be keyed by this name
  std::string name;
  // Should return a new attribution for this attribution format. The attribution
  // should use the given attribution rule and attribution context.
  std::function<std::unique_ptr<
      AttributionOutput>(AttributionRule, AttributionContext, fbpcf::Visibility)>
      newAttributor;
};

AttributionFormat getAttributionFormatFromNameOrThrow(const std::string& name);
AttributionFormat getAttributionFormatFromIdOrThrow(int64_t id);

} // namespace aggregation::private_attribution
