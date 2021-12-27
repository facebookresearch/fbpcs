/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <unordered_set>

#include "fbpcs/emp_games/lift/common/DataFrame.h"
#include "fbpcs/emp_games/lift/common/IDataFrameBuilder.h"

namespace private_lift {
/**
 * This class is a convenience wrapper to construct a DataFrame from an input
 * CSV file meant for Private Lift by adding derived columns, applying caps,
 * precomputing values where necessary, and dropping unnecessary columns to
 * conserve memory usage.
 */
class LiftDataFrameBuilder : public IDataFrameBuilder {
 public:
  /**
   * Construct a new LiftDataFrameBuilder pointing to a specific CSV file and
   * with the specified conversion cap
   *
   * @param filePath the path to the CSV file to read
   * @param conversionCap the maximum number of conversions to support per-user
   */
  LiftDataFrameBuilder(const std::string& filePath, int64_t conversionCap)
      : filePath_{filePath}, conversionCap_{conversionCap} {}

  /**
   * Add testPopulation and controlPopulation columns to a df::DataFrame by:
   * testPopulation = opportunity * test_flag
   * controlPopulation = opportunity * (1 - test_flag)
   *
   * @param df the df::DataFrame to be modified in place
   */
  void addTestControlPopulationColumns(df::DataFrame& df) const;

  /**
   * Limit the number of conversions stored for each user according to the cap
   * specified in the constructor.
   *
   * @param df the df::DataFrame to be modified in place
   */
  void applyConversionCap(df::DataFrame& df) const;

  /**
   * Precompute the total valid value squared at index [i] for each user by
   * applying the math trick of summing all value from `[i, size())` given the
   * property that if conversion[i] is valid, all subsequent conversions must
   * also be valid. For example: if values are [10, 20, 30] then precomputing
   * values squared would yield [(10+20+30)^2, (20+30)^2, 30^2].
   *
   * @param df the df::DataFrame to be modified in place
   */
  void precomputeValuesSquared(df::DataFrame& df) const;

  /**
   * Aggressively drop columns from `df` which are unnecessary for Lift in order
   * to save memory.
   *
   * @param df the df::DataFrame to be modified in place
   */
  void dropUnnecessaryColumns(df::DataFrame& df) const;

  /**
   * Apply all Lift-specific rules to a given df::DataFrame in place.
   *
   * @param df the df::DataFrame to be modified in place
   */
  void applyLiftRules(df::DataFrame& df) const {
    addTestControlPopulationColumns(df);
    applyConversionCap(df);
    precomputeValuesSquared(df);
    dropUnnecessaryColumns(df);
  }

  /**
   * Actualize a new df::DataFrame given this builder's parameterization by
   * reading it from file, applying all standard Lift rules, then returning
   * the resulting df::DataFrame.
   *
   * @returns a df::DataFrame built by reading the given input file and applying
   *     Lift-specific rules for precomputation and setup
   */
  df::DataFrame buildNew() const override {
    auto df = df::DataFrame::readCsv(getLiftTypeMap(), filePath_);
    applyLiftRules(df);
    return df;
  }

  static const df::TypeMap& getLiftTypeMap() {
    static const df::TypeMap kLiftTypeMap{
        // NOTE: opportunity and test_flag *could* be bool columns, but Column
        // doesn't yet supported vectorized bitwise operations, so it's not
        // useful
        .boolColumns = {},
        .intColumns =
            {"opportunity",
             "test_flag",
             "opportunity_timestamp",
             "num_impressions",
             "num_clicks",
             "total_spend",
             "cohort_id",
             "breakdown_id"},
        .intVecColumns = {"event_timestamps", "values"},
    };
    return kLiftTypeMap;
  }

  static const std::unordered_set<std::string>& getNecessaryColumnsForLift() {
    static const std::unordered_set<std::string> kNecessaryColumnsForLift{
        "test_population",
        "control_population",
        "opportunity_timestamp",
        "num_impressions",
        "num_clicks",
        "total_spend",
        "event_timestamps",
        "values",
        "values_squared",
        "cohort_id",
        "breakdown_id"};
    return kNecessaryColumnsForLift;
  }

 private:
  std::string filePath_;
  int64_t conversionCap_;
};

} // namespace private_lift
