/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "fbpcs/emp_games/attribution/decoupled_aggregation/ConversionMetadata.h"
#include "fbpcs/emp_games/attribution/decoupled_aggregation/test/EmpBatcherTestUtil.h"

namespace aggregation::private_aggregation {

TEST(MeasurementConversiontMedataTest, TestBatcherSerialization) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    MeasurementConversionMetadata cm{
        1325 /*conv_value*/
    };

    PrivateMeasurementConversionMetadata convMetadata =
        writeAndReadFromBatcher<PrivateMeasurementConversionMetadata>(cm);

    std::stringstream out;
    out << cm;
    EXPECT_EQ(out.str(), convMetadata.reveal(emp::PUBLIC));
  });
}

} // namespace aggregation::private_aggregation
