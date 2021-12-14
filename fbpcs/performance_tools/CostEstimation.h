/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/dynamic.h>
#include <string>

namespace fbpcs::performance_tools {

// Constants used for fargate container cost computation
const int64_t MEMORY_SIZE = 30;
const int64_t vCPUS = 4;
const double PER_CPU_HOUR_COST =
    0.04656; // Source: https://aws.amazon.com/fargate/pricing/
const double PER_GB_HOUR_COST =
    0.00511; // Source: https://aws.amazon.com/fargate/pricing/
const double NETWORK_PER_GB_COST = 0.01;
const double ECR_PER_GB_COST =
    0.01; // Source: https://aws.amazon.com/ecr/pricing/
const std::string NET_DEV_FILE = "/proc/net/dev";

/*
 * This class estimates the AWS cost of each fargate container.
 */
class CostEstimation {
 private:
  std::string s3Bucket_;
  std::string s3Path_;
  std::string application_;
  double estimatedCost_;
  int64_t runningTimeInSec_;
  long networkRXBytes_; // Network Receive bytes
  long networkTXBytes_; // Network Transmit bytes
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;

 public:
  explicit CostEstimation(const std::string& app);

  std::string& getApplication();
  double& getEstimatedCost();
  long getNetworkBytes();
  void calculateCost();

  std::unordered_map<std::string, long> readNetworkSnapshot();
  std::string getEstimatedCostString();
  folly::dynamic getEstimatedCostDynamic(
      std::string run_name,
      std::string attribution_rules,
      std::string aggregators);
  folly::dynamic getEstimatedCostDynamic(std::string run_name);

  void start();
  void end();

  std::string writeToS3(std::string run_name, folly::dynamic costDynamic);
};

} // namespace fbpcs::performance_tools
