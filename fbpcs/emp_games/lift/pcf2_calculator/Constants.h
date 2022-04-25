/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/frontend/mpcGame.h"

namespace private_lift {

template <int schedulerId, bool usingBatch = true>
using PubBit =
    typename fbpcf::frontend::MpcGame<schedulerId>::template PubBit<usingBatch>;

template <int schedulerId, bool usingBatch = true>
using SecBit =
    typename fbpcf::frontend::MpcGame<schedulerId>::template SecBit<usingBatch>;

} // namespace private_lift
