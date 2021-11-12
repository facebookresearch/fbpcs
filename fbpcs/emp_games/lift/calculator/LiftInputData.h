/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include <emp-tool/emp-tool.h>

#include <fbpcf/mpc/EmpGame.h>

#include "fbpcs/emp_games/lift/calculator/LiftDataFrameBuilder.h"
#include "fbpcs/emp_games/lift/common/Column.h"
#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace private_lift {
/**
 * Class that prepares data for Private Lift from a CSV. Primary interface is
 * via getDf, though getBitmaskFor is also useful as a precomputed vector of
 * bitmasks for calculating subgroup lift.
 */
class LiftInputData {
 public:
  /**
   * Construct a LiftInputData object for a party with a path to a CSV file.
   *
   * @param party the party for which to prepare data
   * @param filePath the path to the CSV with this party's data
   */
  LiftInputData(fbpcf::Party party, const std::string& filePath);

  /**
   * Construct a LiftInputData object for a party with a custom builder. This
   * should only be used if you really know what you're doing. It's useful for
   * extending a builder, but in most cases, the builder-less LiftInputData
   * constructor will be what a developer wants to use directly.
   *
   * @param builder an object which can prepare a Lift DataFrame
   * @param party the party for which to prepare data
   */
  LiftInputData(const LiftDataFrameBuilder& builder, fbpcf::Party party);

  /**
   * Get the prepared `df::DataFrame`.
   *
   * @returns this LiftInputData's `df::DataFrame`
   */
  const df::DataFrame& getDf() const {
    return df_;
  }

  /**
   * Non-const version of `LiftInputData::getDf`. Get the prepared
   * `df::DataFrame`.
   *
   * @returns this LiftInputData's `df::DataFrame`
   */
  df::DataFrame& getDf() {
    return const_cast<df::DataFrame&>(
        const_cast<const LiftInputData&>(*this).getDf());
  }

  /**
   * Retrieve the supported groupKey string for an `fbpcf::Party`.
   *
   * @returns the supported groupKey string for an `fbpcf::Party`
   */
  static std::string getGroupKeyForParty(fbpcf::Party party) {
    // Assumption: Alice == Publisher
    return party == fbpcf::Party::Alice ? std::string{"breakdown_id"}
                                        : std::string{"cohort_id"};
  }

  /**
   * Get this LiftInputData's group count (how many subgroups this party wishes
   * to compute sub-audience lift for). This was previously calculated by
   * finding the maximum id under this input's groupKey. Since subgroups *must*
   * begin from index zero, finding index k means there are at least k groups.
   *
   * @returns the number of known groups in this LiftInputData
   */
  int64_t getGroupCount() const {
    return groupCount_;
  }

  /**
   * Get a column of bits representing a bitmask over a given groupId.
   *
   * @param groupId the groupId for which to retrieve a bitmask column
   * @returns a `df::Column` describing whether row[i] is valid for this group
   * @throws std::out_of_range if groupId > groupCount
   */
  const df::Column<bool>& getBitmaskFor(int64_t groupId) const {
    return bitmasks_.at(groupId);
  }

  /**
   * Calculate the number of groups in this LiftInputData by finding the max
   * id in the groupKey. For more details, see `LiftInputData::getGroupCount`.
   *
   * @returns the calculated number of groups in the `df::DataFrame`
   */
  int64_t calculateGroupCount() const;

  /**
   * Calculate all of the bitmasks for groups defined in this LiftInputData.
   * This is a relatively expensive function, but the intention is that it is
   * run in the LiftInputData constructor to cache the result for later. For
   * more details, see `LiftInputData::getBitmaskFor`.
   *
   * @returns a vector of `Column<bool>` representing whether row[i] is valid
   *     for the group stored in vector index[j]
   */
  std::vector<df::Column<bool>> calculateBitmasks() const;

 private:
  fbpcf::Party party_;
  std::string groupKey_;
  df::DataFrame df_;
  int64_t groupCount_;
  std::vector<df::Column<bool>> bitmasks_;
};
} // namespace private_lift
