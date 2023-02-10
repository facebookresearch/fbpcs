/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/frontend/mpcGame.h"

namespace pcf2_attribution {

const int kMaxConcurrency = 16;
const size_t timeStampWidth = 32;
const size_t targetIdWidth = 64;
const size_t actionTypeWidth = 16;
const size_t originalAdIdWidth = 64;
const size_t adIdWidth = 16;
const size_t convValueWidth = 32;

template <int schedulerId>
using PubBit =
    typename fbpcf::frontend::MpcGame<schedulerId>::template PubBit<true>;
template <int schedulerId>
using SecBit =
    typename fbpcf::frontend::MpcGame<schedulerId>::template SecBit<true>;

template <int schedulerId>
using PubTimestamp = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<timeStampWidth, true>;
template <int schedulerId>
using SecTimestamp = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<timeStampWidth, true>;

template <int schedulerId>
using PubTargetId = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<targetIdWidth, true>;
template <int schedulerId>
using SecTargetId = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<targetIdWidth, true>;

template <int schedulerId>
using PubActionType = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<actionTypeWidth, true>;
template <int schedulerId>
using SecActionType = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<actionTypeWidth, true>;

template <int schedulerId>
using PubOriginalAdId = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<originalAdIdWidth, true>;
template <int schedulerId>
using SecOriginalAdId = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<originalAdIdWidth, true>;

template <int schedulerId>
using PubAdId = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<adIdWidth, true>;
template <int schedulerId>
using SecAdId = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<adIdWidth, true>;

template <int schedulerId>
using PubConvValue = typename fbpcf::frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<convValueWidth, true>;
template <int schedulerId>
using SecConvValue = typename fbpcf::frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<convValueWidth, true>;

} // namespace pcf2_attribution
