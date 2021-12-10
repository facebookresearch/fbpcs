/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

namespace private_measurement::functional {
namespace detail {
// Logic adapted from https://stackoverflow.com/a/18771618/15625637
template <class I>
void advance(I&& it) {
  ++it;
}

template <class I, class... Is>
void advance(I&& it, Is&&... its) {
  ++it;
  detail::advance(its...);
}

// Special template to allow for cases where there *are* no more iterators
// In this case, zip_apply is no different from std::transform, so it is
// unlikely anyone would use it, but it could help avoid a frustrating
// compilation error when testing or debugging something.
template <class... Is>
void advance(Is&&...) { /* empty */
  ;
}
} // namespace detail

/*
 * Acts as a "zip and map" utility with automatic type deduction
 * The first iterator must be <= the size of all other passed iterators
 * otherwise the result is UB caused by iterating beyond end iterators.
 *
 * Example usage:
 * std::vector<int> v1{1, 2, 3, 4, 5};
 * std::vector<int> v2{5, 6, 7, 8, 9};
 * std::vector<int> v3{3, 2, 1, 2, 3};
 * auto res = zip_apply(
 *     v1.begin(), v1.end(),
 *     v2.begin(),
 *     v3.begin(),
 *     [](auto n1, auto n2, auto n3) {
 *         return n1 * n2 - n3;
 *     });
 * EXPECT_EQ(res, std::vector{2, 10, 20, 30, 42});
 */
template <class Function, class I, class... Is>
auto zip_apply(Function f, I&& begin, I&& end, Is&&... its)
    -> std::vector<decltype(f(*begin, *(its)...))> {
  std::vector<decltype(f(*begin, *(its)...))> res;
  // TODO: Would be better to check *all* iterators instead of just first
  // This function assumes the first iterator is the only end checked
  // A better design would take pairs of begin and end iterators
  for (/* empty */; begin != end; ++begin, detail::advance(its...)) {
    res.push_back(f(*begin, *(its)...));
  }
  return res;
}
} // namespace private_measurement::functional
