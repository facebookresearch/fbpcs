/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcf/frontend/mpcGame.h"

namespace pcf2_aggregation {

namespace pcf_frontend = fbpcf::frontend;

enum AGGREGATION_FORMAT { AD_OBJECT_FORMAT = 1 };

const int kMaxConcurrency = 16;

// We are compressing the original Ad Id (64 bit integer), by mapping it to
// an integer in the range 1 - num_of_ad_ids. Assumption here is that the
// number of ad_ids will be less than 65,536 per run.
const size_t adIdWidth = 16;
const size_t originalAdIdWidth = 64;
const size_t convValueWidth = 32;
const size_t salesValueWidth = 32;

template <int schedulerId>
using PubBit = typename pcf_frontend::MpcGame<schedulerId>::template PubBit<>;
template <int schedulerId>
using SecBit = typename pcf_frontend::MpcGame<schedulerId>::template SecBit<>;

template <int schedulerId>
using PubAdId = typename pcf_frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<adIdWidth>;
template <int schedulerId>
using SecAdId = typename pcf_frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<adIdWidth>;

template <int schedulerId>
using PubOriginalAdId = typename pcf_frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<originalAdIdWidth>;
template <int schedulerId>
using SecOriginalAdId = typename pcf_frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<originalAdIdWidth>;

template <int schedulerId>
using PubConvValue = typename pcf_frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<convValueWidth>;
template <int schedulerId>
using SecConvValue = typename pcf_frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<convValueWidth>;

template <int schedulerId>
using PubSalesValue = typename pcf_frontend::MpcGame<
    schedulerId>::template PubUnsignedInt<salesValueWidth>;
template <int schedulerId>
using SecSalesValue = typename pcf_frontend::MpcGame<
    schedulerId>::template SecUnsignedInt<salesValueWidth>;

} // namespace pcf2_aggregation
