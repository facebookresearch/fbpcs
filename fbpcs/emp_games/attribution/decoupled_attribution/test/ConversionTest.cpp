/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "fbpcs/emp_games/attribution/decoupled_attribution/Conversion.h"

namespace aggregation::private_attribution {
TEST(ConversionTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Conversion conv{
        12345 /*ts*/
    };

    PrivateConversion privateConv{conv, emp::ALICE};

    std::stringstream out;
    out << conv;
    EXPECT_EQ(out.str(), privateConv.reveal());
  });
}
} // namespace aggregation::private_attribution
