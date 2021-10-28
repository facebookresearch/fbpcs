/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <emp-sh2pc/emp-sh2pc.h>

namespace aggregation::private_aggregation {

template <typename TOut, typename... Args>
emp::Batcher writeToBatcher(Args&&... args) {
  emp::Batcher myBatcher;
  myBatcher.add<TOut>(std::forward<Args>(args)...);
  myBatcher.make_semi_honest(emp::ALICE);
  return myBatcher;
}

template <typename TOut, typename... Args>
TOut writeAndReadFromBatcher(Args&&... args) {
  emp::Batcher myBatcher = writeToBatcher<TOut>(std::forward<Args>(args)...);
  return myBatcher.next<TOut>();
}

} // namespace aggregation::private_attribution
