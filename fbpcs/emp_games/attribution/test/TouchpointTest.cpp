/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "../Timestamp.h"
#include "../Touchpoint.h"

namespace measurement::private_attribution {
TEST(TouchpointTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Touchpoint tp{
        123 /*id*/,
        true /*isClick*/,
        456 /*adId*/,
        789 /*ts*/,
        10 /*campaignMetadata*/
    };

    PrivateTouchpoint privateTp{tp, emp::ALICE};

    std::stringstream out;
    out << tp;
    EXPECT_EQ(out.str(), privateTp.reveal(emp::PUBLIC));
  });
}
} // namespace measurement::private_attribution
