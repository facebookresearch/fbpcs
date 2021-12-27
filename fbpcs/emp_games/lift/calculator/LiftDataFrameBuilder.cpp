/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/calculator/LiftDataFrameBuilder.h"

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

#include "fbpcs/emp_games/lift/common/Column.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

using namespace private_lift;

void LiftDataFrameBuilder::addTestControlPopulationColumns(
    df::DataFrame& df) const {
  auto keys = df.keys();
  if (keys.find("test_flag") != keys.end()) {
    auto& testFlag = df.get<int64_t>("test_flag");
    df::Column<int64_t> oneColumn(testFlag.size(), 1);
    if (keys.find("opportunity") != keys.end()) {
      auto& opp = df.get<int64_t>("opportunity");
      df.get<int64_t>("test_population") = opp * testFlag;
      df.get<int64_t>("control_population") = opp * (oneColumn - testFlag);
    } else {
      df.get<int64_t>("test_population") = testFlag;
      df.get<int64_t>("control_population") = oneColumn - testFlag;
    }
  }
}

void LiftDataFrameBuilder::applyConversionCap(df::DataFrame& df) const {
  std::vector<std::string> cappedColumnKeys{"event_timestamps", "values"};
  auto keys = df.keys();
  for (const auto& key : cappedColumnKeys) {
    if (keys.find(key) != keys.end()) {
      // We take the *first N* conversions for this user
      // NOTE: This should later be switched to *last N*
      df.get<std::vector<int64_t>>(key).apply(
          [this](auto& innerVec) { innerVec.resize(conversionCap_); });
    }
  }
}

void LiftDataFrameBuilder::precomputeValuesSquared(df::DataFrame& df) const {
  auto keys = df.keys();
  if (keys.find("values") != keys.end()) {
    df.get<std::vector<int64_t>>("values_squared") =
        df.get<std::vector<int64_t>>("values").map([](auto& innerVec) {
          std::vector<int64_t> res(innerVec.size());
          int64_t acc = 0;
          // Reverse iterate to accumulate total value in range [i, end)
          // This may look dumb, but size_t is an unsigned type, so we can't
          // iterate down to zero (it underflows and never terminates)
          std::size_t i = innerVec.size();
          while (i--) {
            acc += innerVec.at(i);
            res.at(i) = acc * acc;
          }
          return res;
        });
  }
}

void LiftDataFrameBuilder::dropUnnecessaryColumns(df::DataFrame& df) const {
  auto keys = df.keys();

  // First find keys not present in the list of necessary columns
  for (const auto& key : getNecessaryColumnsForLift()) {
    keys.erase(key);
  }

  // Then drop them from the DataFrame
  for (const auto& extraColumn : keys) {
    // This code is tricky: since we originally supplied the TypeMap to
    // df::DataFrame::readCsv, we know *for sure* which column types could be
    // present here. If you haphazardly try to drop additional columns, it may
    // cause a SEGV in the downstream application.
    if (std::find(
            getLiftTypeMap().boolColumns.begin(),
            getLiftTypeMap().boolColumns.end(),
            extraColumn) != getLiftTypeMap().boolColumns.end()) {
      df.drop<bool>(extraColumn);
    } else if (
        std::find(
            getLiftTypeMap().intColumns.begin(),
            getLiftTypeMap().intColumns.end(),
            extraColumn) != getLiftTypeMap().intColumns.end()) {
      df.drop<int64_t>(extraColumn);
    } else if (
        std::find(
            getLiftTypeMap().intVecColumns.begin(),
            getLiftTypeMap().intVecColumns.end(),
            extraColumn) != getLiftTypeMap().intVecColumns.end()) {
      df.drop<std::vector<int64_t>>(extraColumn);
    } else {
      // Everything else is std::string
      df.drop<std::string>(extraColumn);
    }
  }
}
