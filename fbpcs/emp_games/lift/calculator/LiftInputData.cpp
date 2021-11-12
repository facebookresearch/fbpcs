/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/calculator/LiftInputData.h"

#include <string>
#include <type_traits>
#include <vector>

#include <emp-tool/emp-tool.h>

#include <fbpcf/mpc/EmpGame.h>

#include "fbpcs/emp_games/lift/calculator/LiftDataFrameBuilder.h"
#include "fbpcs/emp_games/lift/common/Column.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace {
inline constexpr int64_t kConversionCap = 25;
}

namespace private_lift {
LiftInputData::LiftInputData(
    fbpcf::Party party,
    const std::string& filePath)
    : LiftInputData{
          LiftDataFrameBuilder{filePath, kConversionCap},
          party} {}

LiftInputData::LiftInputData(
    const LiftDataFrameBuilder& builder,
    fbpcf::Party party)
    : party_{party}, groupKey_{getGroupKeyForParty(party)} {
  df_ = builder.buildNew();
  groupCount_ = calculateGroupCount();
  bitmasks_ = calculateBitmasks();
}

int64_t LiftInputData::calculateGroupCount() const {
  int64_t maxId = -1;
  auto keys = df_.keys();

  // It's possible that neither group key appears in the dataset - these
  // are optional fields in the input spec
  if (keys.find(groupKey_) != keys.end()) {
    for (const auto& value : df_.at<int64_t>(groupKey_)) {
      maxId = std::max(maxId, value);
    }
  }

  // If neither group key was in this df, this will appropriately set
  // groupCount_ to zero (no groups in dataset)
  // NOTE: Since it's expected that groups start from index 0, if we find a max
  //       id == N, we have N + 1 groups!
  return maxId + 1;
}

std::vector<df::Column<bool>> LiftInputData::calculateBitmasks()
    const {
  std::vector<df::Column<bool>> res;
  for (std::size_t group = 0; group < getGroupCount(); ++group) {
    df::Column<bool> groupColumn;
    for (const auto& value : df_.at<int64_t>(groupKey_)) {
      groupColumn.push_back(value == group);
    }
    res.push_back(std::move(groupColumn));
  }
  return res;
}
} // namespace private_lift
