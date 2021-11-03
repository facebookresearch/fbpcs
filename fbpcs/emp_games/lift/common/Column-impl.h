/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/lift/common/Column.h"

namespace df {

template <typename T, typename T2>
auto operator+(const Column<T> &a, const Column<T2> &b)
    -> Column<decltype(a.at(0) + b.at(0))> {
  return a.mapWith(
      b, [](const T &aValue, const T2 &bValue) { return aValue + bValue; });
}

template <typename T, typename T2>
auto operator+(const Column<T> &a, T2 &b) -> Column<decltype(a.at(0) + b)> {
  return a.mapWithScalar(
      b, [](const T &aValue, const T2 &bValue) { return aValue + bValue; });
}

} // namespace df
