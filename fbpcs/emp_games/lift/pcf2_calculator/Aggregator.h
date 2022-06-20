/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "folly/logging/xlog.h"

#include "fbpcf/mpc_std_lib/oram/IWriteOnlyOramFactory.h"
#include "fbpcf/mpc_std_lib/oram/LinearOramFactory.h"
#include "fbpcf/mpc_std_lib/oram/WriteOnlyOramFactory.h"
#include "fbpcs/emp_games/lift/common/GroupedLiftMetrics.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/Attributor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/InputProcessor.h"
#include "fbpcs/emp_games/lift/pcf2_calculator/OutputMetricsData.h"

namespace private_lift {

template <typename T, bool useVector>
using ConditionalVector =
    typename std::conditional<useVector, std::vector<T>, T>::type;

template <bool isSigned, int8_t width>
using Intp = typename fbpcf::mpc_std_lib::util::Intp<isSigned, width>;

template <bool isSigned, int8_t width>
using NativeIntp =
    typename fbpcf::mpc_std_lib::util::Intp<isSigned, width>::NativeType;

template <int schedulerId, bool isSigned, int8_t width>
using SecInt =
    typename fbpcf::frontend::Int<isSigned, width, true, schedulerId, false>;

template <int schedulerId>
class Aggregator {
 public:
  Aggregator(
      int myRole,
      InputProcessor<schedulerId> inputProcessor,
      std::unique_ptr<Attributor<schedulerId>> attributor,
      int32_t numConversionsPerUser,
      std::shared_ptr<
          fbpcf::engine::communication::IPartyCommunicationAgentFactory>
          communicationAgentFactory)
      : myRole_{myRole},
        inputProcessor_{inputProcessor},
        attributor_{std::move(attributor)},
        numRows_{inputProcessor.getNumRows()},
        numPartnerCohorts_{inputProcessor.getNumPartnerCohorts()},
        numConversionsPerUser_{numConversionsPerUser},
        communicationAgentFactory_{communicationAgentFactory},
        cohortIndexShares_{inputProcessor.getCohortIndexShares()},
        testCohortIndexShares_{inputProcessor.getTestCohortIndexShares()} {
    initOram();
    sumEvents();
    sumConverters();
    sumNumConvSquared();
    sumMatch();
    sumReachedConversions();
    sumValues();
    sumReachedValues();
    sumValueSquared();
  }

  const OutputMetricsData getMetrics() const {
    return metrics_;
  }

  const std::unordered_map<int64_t, OutputMetricsData> getCohortMetrics()
      const {
    return cohortMetrics_;
  }

  std::string toJson() const;

 private:
  void initOram();

  void sumEvents();

  void sumConverters();

  void sumNumConvSquared();

  void sumMatch();

  void sumReachedConversions();

  void sumValues();

  void sumReachedValues();

  void sumValueSquared();

  // Run ORAM aggregation on input. The template parameter useVector indicates
  // whether the input consists of a vector of inputs or a single input.
  template <bool isSigned, int8_t width, bool useVector>
  std::vector<SecInt<schedulerId, isSigned, width>> aggregate(
      const std::vector<std::vector<bool>>& indexShares,
      ConditionalVector<std::vector<std::vector<bool>>, useVector>& valueShares,
      size_t oramSize,
      std::unique_ptr<
          fbpcf::mpc_std_lib::oram::IWriteOnlyOram<Intp<isSigned, width>>> oram)
      const;

  // Reveal cohort output from aggregation output as a tuple consisting of the
  // test/control metrics, the test cohort metrics, and the control cohort
  // metrics.
  template <bool isSigned, int8_t width>
  std::tuple<
      std::vector<NativeIntp<isSigned, width>>,
      std::vector<NativeIntp<isSigned, width>>,
      std::vector<NativeIntp<isSigned, width>>>
  revealCohortOutput(std::vector<SecInt<schedulerId, isSigned, width>>
                         aggregationOutput) const;

  // Reveal cohort output from aggregation output as a tuple consisting of the
  // test metrics and the test cohort metrics.
  template <bool isSigned, int8_t width>
  std::tuple<
      NativeIntp<isSigned, width>,
      std::vector<NativeIntp<isSigned, width>>>
  revealTestCohortOutput(std::vector<SecInt<schedulerId, isSigned, width>>
                             aggregationOutput) const;

  int32_t myRole_;
  InputProcessor<schedulerId> inputProcessor_;
  std::unique_ptr<Attributor<schedulerId>> attributor_;
  int64_t numRows_;
  uint32_t numPartnerCohorts_;
  int32_t numConversionsPerUser_;
  uint32_t numCohortGroups_;
  uint32_t numTestCohortGroups_;
  OutputMetricsData metrics_;

  std::shared_ptr<fbpcf::engine::communication::IPartyCommunicationAgentFactory>
      communicationAgentFactory_;
  std::unique_ptr<
      fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<Intp<false, valueWidth>>>
      cohortUnsignedWriteOnlyOramFactory_;
  std::unique_ptr<
      fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<Intp<true, valueWidth>>>
      cohortSignedWriteOnlyOramFactory_;
  std::unique_ptr<
      fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<Intp<false, valueWidth>>>
      testCohortUnsignedWriteOnlyOramFactory_;
  std::unique_ptr<
      fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<Intp<true, valueWidth>>>
      testCohortSignedWriteOnlyOramFactory_;
  std::unique_ptr<fbpcf::mpc_std_lib::oram::IWriteOnlyOramFactory<
      Intp<false, valueSquaredWidth>>>
      valueSquaredWriteOnlyOramFactory_;

  std::vector<std::vector<bool>> cohortIndexShares_;
  std::vector<std::vector<bool>> testCohortIndexShares_;
  std::unordered_map<int64_t, OutputMetricsData> cohortMetrics_;
  std::unordered_map<int64_t, OutputMetricsData>
      publisherBreakdowns_; // place holder for publisher breakdown metrics.
};
} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/Aggregator_impl.h"
