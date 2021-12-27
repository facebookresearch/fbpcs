/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/common/DataFrame.h"

namespace private_lift {
/**
 * Interface for a class which is able to build a new DataFrame in some context.
 * It's useful to define this as an interface for easier testability in classes
 * which may wish to use a DataFrameBuilder, but would benefit from mocking the
 * implementation to return static data in test classes.
 */
class IDataFrameBuilder {
 public:
  /**
   * Virtual destructor since this class is an interface.
   */
  virtual ~IDataFrameBuilder() {}

  /**
   * Construct a new `df::DataFrame` (*not* a reference to an existing object).
   *
   * @returns a newly constructed `df::DataFrame`
   */
  virtual df::DataFrame buildNew() const = 0;
};
} // namespace private_lift
