/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/common/ColumnNameConstants.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace private_lift {
/**
 * This struct represents a single row of Lift data. It is meant to be used in
 * conjunction with DataFrame objects along with an iterator pattern in order
 * to enable row-wise traversal of a DataFrame with columns required for lift.
 *
 * @tparam BitType the type used internally to represent bits/boolean values
 * @tparam IntType the type used internally to represent integer values
 */
template <typename BitType, typename IntType>
struct LiftRow {
  // Required publisher columns
  const IntType* opportunityTimestamp;
  const BitType* testPopulation;
  const BitType* controlPopulation;
  const BitType* reachedPopulation;
  // Optional publisher columns
  const IntType* breakdownId;

  // Required partner columns
  const BitType* partnerRow;
  const std::vector<IntType>* eventTimestamps;
  const std::vector<IntType>* values;
  const std::vector<IntType>* valuesSquared;
  // Optional partner columns
  const IntType* cohortId;

  /**
   * Construct a LiftRow from a DataFrame at the given index. This functionality
   * allows us to later actualize a row-wise DataFrame iterator.
   *
   * @param dframe the DataFrame from which to pull row data
   * @param idx the exact index into the DataFrame from which to pull row data
   * @returns a LiftRow representing a view into `dframe.at(idx)`
   */
  static LiftRow fromDataFrame(const df::DataFrame& dframe, std::size_t idx) {
    LiftRow row;
    row.opportunityTimestamp =
        &dframe.get<IntType>(lift_columns::kOpportunityTimestamp).at(idx);
    row.testPopulation =
        &dframe.get<BitType>(lift_columns::kTestPopulation).at(idx);
    row.controlPopulation =
        &dframe.get<BitType>(lift_columns::kControlPopulation).at(idx);
    row.reachedPopulation =
        &dframe.get<BitType>(lift_columns::kReached).at(idx);

    row.partnerRow = &dframe.get<BitType>(lift_columns::kPartnerRow).at(idx);
    row.eventTimestamps =
        &dframe.get<std::vector<IntType>>(lift_columns::kEventTimestamps)
             .at(idx);
    row.values =
        &dframe.get<std::vector<IntType>>(lift_columns::kValues).at(idx);
    row.valuesSquared =
        &dframe.get<std::vector<IntType>>(lift_columns::kValuesSquared).at(idx);

    // breakdownId and cohortId are both optional
    row.breakdownId = dframe.containsKey(lift_columns::kBreakdownId)
        ? &dframe.get<IntType>(lift_columns::kBreakdownId).at(idx)
        : nullptr;

    row.cohortId = dframe.containsKey(lift_columns::kCohortId)
        ? &dframe.get<IntType>(lift_columns::kCohortId).at(idx)
        : nullptr;

    return row;
  }
};
} // namespace private_lift
