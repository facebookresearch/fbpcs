/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <variant>
#include <vector>

#include <emp-sh2pc/emp-sh2pc.h>

#include <folly/dynamic.h>
#include <folly/logging/xlog.h>

namespace private_measurement {
enum class AggMetricsTag {
  Integer,
  EmpInteger,
  List,
  Map,
};

class AggMetrics {
 public:
  using MetricsInt = int64_t;
  using MetricsList = std::vector<std::shared_ptr<AggMetrics>>;
  using MetricsMap = std::map<std::string, std::shared_ptr<AggMetrics>>;
  using MetricsValue =
      std::variant<MetricsInt, emp::Integer, MetricsList, MetricsMap>;

  explicit AggMetrics(int64_t value)
      : tag_{AggMetricsTag::Integer}, value_{value} {}

  explicit AggMetrics(emp::Integer value)
      : tag_{AggMetricsTag::EmpInteger}, value_{value} {}

  // only use tag constructor when creating List or Map type
  explicit AggMetrics(AggMetricsTag tag) : tag_{tag} {
    if (tag == AggMetricsTag::List) {
      value_ = std::vector<std::shared_ptr<AggMetrics>>{};

    } else if (tag == AggMetricsTag::Map) {
      value_ = std::map<std::string, std::shared_ptr<AggMetrics>>{};

    } else {
      XLOG(FATAL)
          << "AggMetrics should be constructed with explicit value when not constructing a "
          << "List or Map";
    }
  }

  AggMetricsTag getTag() const;
  MetricsValue getValue() const;

  MetricsInt getIntValue() const;
  emp::Integer getEmpIntValue() const;
  void setEmpIntValue(emp::Integer value);

  // list accessors/mutators
  const MetricsList& getAsList() const;
  std::shared_ptr<AggMetrics> getAtIndex(std::size_t i) const;
  void pushBack(std::shared_ptr<AggMetrics> value);

  // map accessors/mutators
  const MetricsMap& getAsMap() const;
  std::shared_ptr<AggMetrics> getAtKey(const std::string& key) const;
  void emplace(std::string key, std::shared_ptr<AggMetrics> value);

  static AggMetrics fromDynamic(const folly::dynamic& obj);
  folly::dynamic toDynamic() const;

  void printSpaces(std::ostream& os, int32_t n) const;
  void print(std::ostream& os, int32_t tabbing = 0) const;
  friend std::ostream& operator<<(
      std::ostream& os,
      const private_measurement::AggMetrics& metrics);

  static std::shared_ptr<AggMetrics> copy(
      const std::shared_ptr<AggMetrics>& metrics);
  void mergeWithViaAddition(const std::shared_ptr<AggMetrics>& metrics);

 private:
  AggMetricsTag tag_;
  MetricsValue value_;

  void checkMyType(AggMetricsTag tag) const;
};
} // namespace private_measurement
