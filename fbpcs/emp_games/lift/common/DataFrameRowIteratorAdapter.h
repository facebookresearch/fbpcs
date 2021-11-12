/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace df {
template <typename RowType>
class DataFrameRowIteratorAdapter {
 public:
  explicit DataFrameRowIteratorAdapter(DataFrame& df) : df_{df} {}

  /**
   * Create an adapted begin iterator to this Adapter's referenced DataFrame.
   * Since this class is templated, we have a "pure" begin function which can
   * be used in for-each constructs. See the code in
   * DataFrameTest::RowIteratorTest::RowIteratorAdapter for an example.
   */
  DataFrame::RowIterator<RowType> begin() {
    return df_.begin<RowType>();
  }

  /**
   * Create an adapted end iterator to this Adapter's referenced DataFrame.
   *
   * @returns a DataFrame::RowIteratorEndSentinel to enable for-each iteration
   */
  DataFrame::RowIteratorEndSentinel end() {
    return df_.end();
  }

 private:
  DataFrame& df_;
};
} // namespace df
