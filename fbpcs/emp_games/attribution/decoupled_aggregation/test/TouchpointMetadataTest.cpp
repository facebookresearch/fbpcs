/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "fbpcs/emp_games/attribution/decoupled_aggregation/TouchPointMetadata.h"

namespace aggregation::private_aggregation {

TEST(MeasurementTouchpointMedataTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    MeasurementTouchpointMedata tp{
        456 /*adId*/
    };

    PrivateMeasurementTouchpointMetadata privateTp{tp, emp::ALICE};

    std::stringstream out;
    out << tp;
    EXPECT_EQ(out.str(), privateTp.reveal(emp::PUBLIC));
  });
}

} // namespace aggregation::private_aggregation
