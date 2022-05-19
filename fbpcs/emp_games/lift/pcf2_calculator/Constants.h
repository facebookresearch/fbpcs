/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/frontend/mpcGame.h"

namespace private_lift {

const int kMaxConcurrency = 16;

const size_t groupWidth = 6; // at most 32 cohorts
const size_t numConvSquaredWidth = 32;
const size_t valueWidth = 32;
const size_t valueSquaredWidth = 64;
const size_t timeStampWidth = 32;

template <int schedulerId, bool usingBatch = true>
using PubBit =
    typename fbpcf::frontend::MpcGame<schedulerId>::template PubBit<usingBatch>;

template <int schedulerId, bool usingBatch = true>
using SecBit =
    typename fbpcf::frontend::MpcGame<schedulerId>::template SecBit<usingBatch>;

template <int schedulerId, bool usingBatch = true>
using PubGroup = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<groupWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using SecGroup = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<groupWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using PubValue = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubSignedInt<valueWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using SecValue = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecSignedInt<valueWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using PubValueSquared = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubSignedInt<valueSquaredWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using SecValueSquared = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecSignedInt<valueSquaredWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using PubTimestamp = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<timeStampWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using SecTimestamp = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<timeStampWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using PubNumConvSquared = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<numConvSquaredWidth, usingBatch>;

template <int schedulerId, bool usingBatch = true>
using SecNumConvSquared = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<numConvSquaredWidth, usingBatch>;

} // namespace private_lift
