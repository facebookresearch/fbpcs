/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>

#include <fbpcf/mpc/EmpTestUtil.h>

#include "../Conversion.h"
#include "../Timestamp.h"
#include "EmpBatcherTestUtil.h"


namespace measurement::private_attribution {
TEST(ConversionTest, TestBatcherSerialization) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Conversion conv{
        12345 /*ts*/,
        67 /*conv_value*/,
        8910 /*conv_metadata*/,
    };

    PrivateConversion privateConv =
        writeAndReadFromBatcher<PrivateConversion>(conv);

    std::stringstream out;
    out << conv;
    EXPECT_EQ(out.str(), privateConv.reveal());
  });
}
} // namespace measurement::private_attribution
