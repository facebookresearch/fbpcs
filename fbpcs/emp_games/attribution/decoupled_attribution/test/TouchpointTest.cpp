/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "fbpcs/emp_games/attribution/decoupled_attribution/Timestamp.h"
#include "fbpcs/emp_games/attribution/decoupled_attribution/Touchpoint.h"

namespace aggregation::private_attribution {
TEST(TouchpointTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Touchpoint tp{
        456 /*adId*/,
        true /*isClick*/,
        789 /*ts*/,
    };

    PrivateTouchpoint privateTp{tp, emp::ALICE};

    std::stringstream out;
    out << tp;
    EXPECT_EQ(out.str(), privateTp.reveal(emp::PUBLIC));
  });
}
} // namespace aggregation::private_attribution
