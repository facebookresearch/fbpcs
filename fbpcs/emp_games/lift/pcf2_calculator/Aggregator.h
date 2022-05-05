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

template <bool isSigned, int8_t width>
using Intp = typename fbpcf::mpc_std_lib::util::Intp<isSigned, width>;

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
  }

  const OutputMetricsData getMetrics() const {
    return metrics_;
  }

  const std::unordered_map<int64_t, OutputMetricsData> getCohortMetrics()
      const {
    return cohortMetrics_;
  }

 private:
  void initOram();

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

  std::vector<std::vector<bool>> cohortIndexShares_;
  std::vector<std::vector<bool>> testCohortIndexShares_;
  std::unordered_map<int64_t, OutputMetricsData> cohortMetrics_;
};
} // namespace private_lift

#include "fbpcs/emp_games/lift/pcf2_calculator/Aggregator_impl.h"
