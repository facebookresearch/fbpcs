/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
#include <folly/logging/xlog.h>

#include "fbpcs/emp_games/lift/calculator/LiftDataFrameBuilder.h"
#include "fbpcs/emp_games/lift/common/Column.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace {
// TODO: Move this back to being a configurable variable
inline constexpr int64_t kConversionCap = 25;
}

namespace private_lift {
LiftInputData::LiftInputData(fbpcf::Party party, const std::string& filePath)
    : LiftInputData{LiftDataFrameBuilder{filePath, kConversionCap}, party} {}

LiftInputData::LiftInputData(
    const LiftDataFrameBuilder& builder,
    fbpcf::Party party)
    : party_{party}, groupKey_{getGroupKeyForParty(party)} {
  XLOG(INFO) << "Building DataFrame...";
  df_ = builder.buildNew();
  XLOG(INFO) << "\tDataFrame built.";

  XLOG(INFO) << "Calculating group count...";
  groupCount_ = calculateGroupCount();
  XLOG(INFO) << "\tHave " << groupCount_ << " groups.";

  XLOG(INFO) << "Precalculating bitmasks...";
  bitmasks_ = calculateBitmasks();
  XLOG(INFO) << "\tBitmasks precalculated.";

  XLOG(INFO) << "Calculating total size...";
  size_ = calculateSize();
  XLOG(INFO) << "\tSize is " << size_ << " rows.";

  XLOG(INFO) << "Done constructing LiftInputData.";
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

std::vector<df::Column<bool>> LiftInputData::calculateBitmasks() const {
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

std::size_t LiftInputData::calculateSize() const {
  if (df_.containsKey("opportunity_timestamp")) {
    return df_.at<int64_t>("opportunity_timestamp").size();
  } else {
    // This must be the partner
    return df_.at<std::vector<int64_t>>("event_timestamps").size();
  }
}
} // namespace private_lift
