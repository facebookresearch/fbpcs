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
#include "EmpBatcherTestUtil.h"

namespace measurement::private_attribution {

TEST(TimestampTest, TestLength) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Timestamp ts1{86400};
    EXPECT_EQ(ts1.length(), 64);

    Timestamp ts2{100, emp::PUBLIC, 0, 15359, Precision::MINUTES};
    EXPECT_EQ(ts2.length(), 8);
  });
}

TEST(TimestampTest, TestGeq) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Timestamp ts1{1000};
    Timestamp ts2{600};

    EXPECT_TRUE(ts1.geq(ts2).reveal<bool>());
    EXPECT_TRUE(ts1.geq(ts1).reveal<bool>());
    EXPECT_FALSE(ts2.geq(ts1).reveal<bool>());
  });
}

TEST(TimestampTest, TestEqual) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Timestamp ts1{1000};
    Timestamp ts2{600};

    EXPECT_TRUE(ts1.equal(ts1).reveal<bool>());
    EXPECT_FALSE(ts1.equal(ts2).reveal<bool>());
  });
}

TEST(TimestampTest, TestSelect) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Timestamp ts1{1000};
    Timestamp ts2{600};

    EXPECT_TRUE(ts1.select(emp::Bit(true), ts2).equal(ts2).reveal<bool>());
    EXPECT_TRUE(ts1.select(emp::Bit(false), ts2).equal(ts1).reveal<bool>());
  });
}

TEST(TimestampTest, TestReveal) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Timestamp ts1{1000};
    EXPECT_EQ(ts1.reveal<int64_t>(), 1000);
    EXPECT_EQ(ts1.reveal<std::string>(), "1000");

    Timestamp ts2{3000, emp::PUBLIC, -65536, 65535, Precision::MINUTES};
    EXPECT_EQ(ts2.reveal<int64_t>(), 3000);
    EXPECT_EQ(ts2.reveal<std::string>(), "3000");
  });
}

TEST(TimestampTest, TestMinus) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    Timestamp ts1{1000};
    Timestamp ts2{600};
    Timestamp ts3{400};

    EXPECT_TRUE((ts1 - ts2 == ts3).reveal<bool>());
  });
}

TEST(TimestampTest, TestBatcherSerialization) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    int64_t time = 10800;

    // Default min/max/precision
    Timestamp ts1 = writeAndReadFromBatcher<Timestamp>(time);
    EXPECT_EQ(time, ts1.reveal<int64_t>());

    // Explicitly specified min/max/precision
    auto minValue = 0;
    auto maxValue = 36000;
    auto precision = Precision::HOURS;
    auto batcher =
        writeToBatcher<Timestamp>(time, minValue, maxValue, precision);
    Timestamp ts2 = Timestamp{batcher.label_ptr, minValue, maxValue, precision};
    EXPECT_EQ(time, ts2.reveal<int64_t>());
  });
}

TEST(TimestampTest, TestBitsNeeded) {
  int64_t minInt64 = std::numeric_limits<int64_t>::min();
  int64_t maxInt64 = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(bitsNeeded(minInt64, maxInt64, Precision::SECONDS), 64);
  EXPECT_EQ(bitsNeeded(minInt64, maxInt64, Precision::MINUTES), 59);
  EXPECT_EQ(bitsNeeded(minInt64, maxInt64, Precision::HOURS), 53);

  int32_t minInt32 = std::numeric_limits<int32_t>::min();
  int32_t maxInt32 = std::numeric_limits<int32_t>::max();
  EXPECT_EQ(bitsNeeded(minInt32, maxInt32, Precision::SECONDS), 32);
  EXPECT_EQ(bitsNeeded(minInt32, maxInt32, Precision::MINUTES), 27);
  EXPECT_EQ(bitsNeeded(minInt32, maxInt32, Precision::HOURS), 21);

  EXPECT_EQ(bitsNeeded(1000, 1000, Precision::SECONDS), 0);
  EXPECT_EQ(bitsNeeded(1000, 1001, Precision::SECONDS), 1);
  EXPECT_EQ(bitsNeeded(1000, 1002, Precision::SECONDS), 2);
  EXPECT_EQ(bitsNeeded(1000, 30000, Precision::SECONDS), 15);

  EXPECT_EQ(bitsNeeded(2000, 2059, Precision::MINUTES), 0);
  EXPECT_EQ(bitsNeeded(2000, 2060, Precision::MINUTES), 1);
  EXPECT_EQ(bitsNeeded(2000, 2119, Precision::MINUTES), 1);
  EXPECT_EQ(bitsNeeded(2000, 2120, Precision::MINUTES), 2);
  EXPECT_EQ(bitsNeeded(2000, 30000, Precision::MINUTES), 9);

  EXPECT_EQ(bitsNeeded(3000, 6599, Precision::HOURS), 0);
  EXPECT_EQ(bitsNeeded(3000, 6600, Precision::HOURS), 1);
  EXPECT_EQ(bitsNeeded(3000, 10199, Precision::HOURS), 1);
  EXPECT_EQ(bitsNeeded(3000, 10200, Precision::HOURS), 2);
  EXPECT_EQ(bitsNeeded(3000, 3000000, Precision::HOURS), 10);
}

TEST(TimestampTest, TestScale) {
  int64_t minInt64 = std::numeric_limits<int64_t>::min();
  int64_t maxInt64 = std::numeric_limits<int64_t>::max();

  EXPECT_EQ(scale(minInt64, maxInt64, Precision::SECONDS, maxInt64), maxInt64);
  EXPECT_EQ(scale(minInt64, maxInt64, Precision::SECONDS, minInt64), minInt64);

  EXPECT_EQ(
      scale(minInt64, maxInt64, Precision::MINUTES, maxInt64), maxInt64 / 60);
  EXPECT_EQ(
      scale(minInt64, maxInt64, Precision::MINUTES, minInt64),
      minInt64 / 60 - 1);

  EXPECT_EQ(
      scale(minInt64, maxInt64, Precision::HOURS, maxInt64), maxInt64 / 3600);
  EXPECT_EQ(
      scale(minInt64, maxInt64, Precision::HOURS, minInt64),
      minInt64 / 3600 - 1);

  EXPECT_EQ(scale(100, 300, Precision::SECONDS, 200), 0);
  EXPECT_EQ(scale(100, 300, Precision::SECONDS, 90), -100);
  EXPECT_EQ(scale(100, 300, Precision::SECONDS, 1000), 100);

  EXPECT_EQ(scale(100, 1100, Precision::MINUTES, 159), -8);
  EXPECT_EQ(scale(100, 1100, Precision::MINUTES, 700), 1);

  EXPECT_EQ(scale(100, 10000, Precision::HOURS, 3700), -1);
  EXPECT_EQ(scale(100, 10000, Precision::HOURS, 9000), 1);
}

TEST(TimestampTest, TestUnscale) {
  int64_t minInt64 = std::numeric_limits<int64_t>::min();
  int64_t maxInt64 = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(
      unscale(minInt64, maxInt64, Precision::SECONDS, maxInt64), maxInt64);
  EXPECT_EQ(
      unscale(minInt64, maxInt64, Precision::SECONDS, minInt64), minInt64);

  EXPECT_EQ(unscale(100, 1000, Precision::SECONDS, 0), 550);
  EXPECT_EQ(unscale(100, 1000, Precision::SECONDS, 400), 950);

  EXPECT_EQ(unscale(100, 1000, Precision::MINUTES, 3), 730);
  EXPECT_EQ(unscale(100, 1000, Precision::MINUTES, -4), 310);

  EXPECT_EQ(unscale(100, 10000, Precision::HOURS, 1), 8650);
  EXPECT_EQ(unscale(100, 10000, Precision::HOURS, -1), 1450);
}
} // namespace measurement::private_attribution
