/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <emp-sh2pc/emp-sh2pc.h>
#include <gtest/gtest.h>
#include <array>

#include <folly/dynamic.h>
#include <cstdlib>

#include <fbpcf/mpc/EmpGame.h>
#include <fbpcf/mpc/EmpTestUtil.h>

#include "../Aggregator.h"

using namespace ::testing;

namespace measurement::private_attribution {
TEST(AemConvMetricTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    AemConvMetric metric{
        12345 /*int64_t campaign_bits*/,
        {0, 1, 2, 3} /*std::vector<int64_t> conversion_bits*/,
        {true, true, false, false} /*std::vector<bool> is_attributed*/,
    };

    emp::Integer camp_bits{INT_SIZE, metric.campaign_bits, emp::ALICE};
    std::vector<emp::Integer> conv_bits;
    for (auto& it : metric.conversion_bits) {
      conv_bits.push_back(emp::Integer{INT_SIZE, it, emp::ALICE});
    }
    std::vector<emp::Bit> is_attr;
    for (auto it : metric.is_attributed) {
      is_attr.push_back(emp::Bit(it, emp::ALICE));
    }

    PrivateAemConvMetric privateAemMetric{camp_bits, conv_bits, is_attr};
    AemConvMetric revealed_metric =
        privateAemMetric.reveal(fbpcf::Visibility::Public);

    EXPECT_EQ(
        metric.toStringConvertionBits(),
        revealed_metric.toStringConvertionBits());

    EXPECT_EQ(
        metric.toStringIsAttributed(), revealed_metric.toStringIsAttributed());

    EXPECT_EQ(metric.toDynamic(), revealed_metric.toDynamic());
    EXPECT_EQ(
        metric.fromDynamic(revealed_metric.toDynamic()).campaign_bits,
        metric.campaign_bits);
    EXPECT_EQ(
        metric.fromDynamic(revealed_metric.toDynamic()).conversion_bits,
        metric.conversion_bits);
    EXPECT_EQ(
        metric.fromDynamic(revealed_metric.toDynamic()).is_attributed,
        metric.is_attributed);
  });
}

TEST(PcmMetricsTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    PcmMetrics metric{
        54321 /*int64_t campaign_bits*/,
        4 /*int64_t conversion_bits*/,
        12345 /*int64_t count*/,
    };

    emp::Integer camp_bits{INT_SIZE, metric.campaign_bits, emp::BOB};
    emp::Integer conv_bits{INT_SIZE, metric.conversion_bits, emp::BOB};
    emp::Integer count{INT_SIZE, metric.count, emp::BOB};

    PrivatePcmMetrics privatePcmMetric{camp_bits, conv_bits, count};
    PcmMetrics revealed_metric =
        privatePcmMetric.reveal(fbpcf::Visibility::Public);

    EXPECT_EQ(metric.toDynamic(), revealed_metric.toDynamic());
    EXPECT_EQ(
        metric.fromDynamic(revealed_metric.toDynamic()).campaign_bits,
        metric.campaign_bits);
    EXPECT_EQ(
        metric.fromDynamic(revealed_metric.toDynamic()).conversion_bits,
        metric.conversion_bits);
    EXPECT_EQ(
        metric.fromDynamic(revealed_metric.toDynamic()).count, metric.count);
  });
}

TEST(ConvMetricsMetricsTest, TestConstructor) {
  fbpcf::mpc::wrapTest<std::function<void()>>([]() {
    ConvMetrics metric{
        1357 /*int64_t convs*/,
        2468 /*int64_t sales*/,
    };

    emp::Integer convs{INT_SIZE, metric.convs, emp::BOB};
    emp::Integer sales{INT_SIZE, metric.sales, emp::BOB};

    PrivateConvMetrics privateConvMetric{convs, sales};
    ConvMetrics revealed_metric =
        privateConvMetric.reveal(fbpcf::Visibility::Public);

    EXPECT_EQ(metric.toDynamic(), revealed_metric.toDynamic());
    EXPECT_EQ(
        metric.fromDynamic(revealed_metric.toDynamic()).convs, metric.convs);
    EXPECT_EQ(
        metric.fromDynamic(revealed_metric.toDynamic()).sales, metric.sales);

    emp::Integer convs_other{INT_SIZE, 4321, emp::ALICE};
    emp::Integer sales_other{INT_SIZE, 32777, emp::ALICE};
    PrivateConvMetrics privateConvMetricOther{convs_other, sales_other};
    PrivateConvMetrics privateConvMetricOrResult{
        convs ^ convs_other, sales ^ sales_other};
    EXPECT_EQ(
        (privateConvMetric ^ privateConvMetricOther)
            .reveal(fbpcf::Visibility::Public)
            .convs,
        privateConvMetricOrResult.reveal(fbpcf::Visibility::Public).convs);
    EXPECT_EQ(
        (privateConvMetric ^ privateConvMetricOther)
            .reveal(fbpcf::Visibility::Public)
            .sales,
        privateConvMetricOrResult.reveal(fbpcf::Visibility::Public).sales);

    PrivateConvMetrics privateConvMetricAddResult{
        convs + convs_other, sales + sales_other};
    EXPECT_EQ(
        (privateConvMetric + privateConvMetricOther)
            .reveal(fbpcf::Visibility::Public)
            .convs,
        privateConvMetricAddResult.reveal(fbpcf::Visibility::Public).convs);
    EXPECT_EQ(
        (privateConvMetric + privateConvMetricOther)
            .reveal(fbpcf::Visibility::Public)
            .sales,
        privateConvMetricAddResult.reveal(fbpcf::Visibility::Public).sales);

    folly::dynamic metricDynamic = metric.toDynamic();
    PrivateConvMetrics xoredFromDynamicResult =
        privateConvMetricOther.xoredFromDynamic(metricDynamic);
    EXPECT_EQ(
        xoredFromDynamicResult.reveal(fbpcf::Visibility::Public).convs, 0);
    EXPECT_EQ(
        xoredFromDynamicResult.reveal(fbpcf::Visibility::Public).sales, 0);
  });
}
} // namespace measurement::private_attribution
