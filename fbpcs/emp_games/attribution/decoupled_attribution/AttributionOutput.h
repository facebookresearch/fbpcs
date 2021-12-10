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

#include <fbpcf/mpc/EmpGame.h>
#include "fbpcs/emp_games/attribution/decoupled_attribution/AttributionRule.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Conversion.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Touchpoint.h"

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

struct PrivateOutputMetricDefault {
  emp::Bit is_attributed{false, emp::PUBLIC};

  OutputMetricDefault reveal(fbpcf::Visibility outputVisibility) const {
    int party =
        outputVisibility == fbpcf::Visibility::Xor ? emp::XOR : emp::PUBLIC;

    return OutputMetricDefault{is_attributed.reveal<bool>(party)};
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
  // Should return a new attribution for this attribution format. The
  // attribution should use the given attribution rule and attribution context.
  std::function<std::unique_ptr<AttributionOutput>(
      AttributionRule,
      AttributionContext,
      fbpcf::Visibility)>
      newAttributor;
};

AttributionFormat getAttributionFormatFromNameOrThrow(const std::string& name);
AttributionFormat getAttributionFormatFromIdOrThrow(int64_t id);

} // namespace aggregation::private_attribution
