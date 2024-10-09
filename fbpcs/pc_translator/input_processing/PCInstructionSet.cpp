/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/pc_translator/input_processing/PCInstructionSet.h"
#include <fbpcf/mpc_std_lib/oram/encoder/IFilter.h>
#include <folly/json.h>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace pc_translator {

using IFilter = fbpcf::mpc_std_lib::oram::IFilter;
const std::vector<std::string>& PCInstructionSet::getGroupByIds() const {
  return groupByIds;
}

const std::vector<FilterConstraint>& PCInstructionSet::getFilterConstraints()
    const {
  return filterConstraints;
}

PCInstructionSet PCInstructionSet::fromDynamic(const folly::dynamic& obj) {
  PCInstructionSet pcInstructionSet;
  auto aggregationConfig = obj["aggregated_metrics"];
  auto groupByFields = aggregationConfig["group_by"];

  for (auto groupByField : groupByFields) {
    pcInstructionSet.groupByIds.push_back(groupByField.asString());
  }

  auto filterConstraintsFields = aggregationConfig["filter"];

  for (auto& [key, constraints] : filterConstraintsFields.items()) {
    std::string name = key.asString();
    for (auto constraint : constraints) {
      auto constraintType = constraint["constraint_type"].asString();
      auto constraintValue = constraint["value"].asInt();

      std::map<std::string, IFilter::FilterType> filterMap = {
          {"EQ", IFilter::FilterType::EQ},
          {"NEQ", IFilter::FilterType::NEQ},
          {"LT", IFilter::FilterType::LT},
          {"LTE", IFilter::FilterType::LTE},
          {"GT", IFilter::FilterType::GT},
          {"GTE", IFilter::FilterType::GTE}};

      auto it = filterMap.find(constraintType);
      if (it == filterMap.end()) {
        throw std::invalid_argument(
            "Constraint type must be - GT, LT, GTE, LTE, EQ, NEQ");
      }

      FilterConstraint filterConstraint(name, it->second, constraintValue);
      pcInstructionSet.filterConstraints.push_back(filterConstraint);
    }
  }

  return pcInstructionSet;
}

} // namespace pc_translator
