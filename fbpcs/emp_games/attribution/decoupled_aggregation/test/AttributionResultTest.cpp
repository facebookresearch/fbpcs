/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "fbpcs/emp_games/attribution/decoupled_aggregation/AttributionResult.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/test/EmpBatcherTestUtil.h"

namespace aggregation::private_aggregation {

TEST(AttributionResultTest, TestBatcherSerialization) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    AttributionResult ar{
        true /*is_attributed*/
    };

    PrivateAttributionResult private_ar =
        writeAndReadFromBatcher<PrivateAttributionResult>(ar);

    std::stringstream out;
    out << ar;
    EXPECT_EQ(out.str(), private_ar.reveal(emp::PUBLIC));
  });
}

} // namespace aggregation::private_aggregation
