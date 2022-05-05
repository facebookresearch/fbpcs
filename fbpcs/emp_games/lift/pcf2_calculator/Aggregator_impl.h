/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "fbpcs/emp_games/common/Util.h"
namespace private_lift {

template <int schedulerId>
void Aggregator<schedulerId>::initOram() {
  // Initialize ORAM
  bool isPublisher = (myRole_ == common::PUBLISHER);
  numCohortGroups_ = std::max(2 * numPartnerCohorts_, uint32_t(2));

  if (numCohortGroups_ > 4) {
    // If the ORAM size is larger than 4, linear ORAM is less efficient
    // theoretically
    cohortUnsignedWriteOnlyOramFactory_ =
        fbpcf::mpc_std_lib::oram::getSecureWriteOnlyOramFactory<
            Intp<false, valueWidth>,
            groupWidth,
            schedulerId>(isPublisher, 0, 1, *communicationAgentFactory_);
  } else {
    cohortUnsignedWriteOnlyOramFactory_ = fbpcf::mpc_std_lib::oram::
        getSecureLinearOramFactory<Intp<false, valueWidth>, schedulerId>(
            isPublisher, 0, 1, *communicationAgentFactory_);
  }
}

} // namespace private_lift
