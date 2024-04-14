/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
    0.04048; // Source: https://aws.amazon.com/fargate/pricing/
const double PER_GB_HOUR_COST =
    0.004445; // Source: https://aws.amazon.com/fargate/pricing/
const double NETWORK_PER_GB_COST = 0.01;
const double ECR_PER_GB_COST =
    0.01; // Source: https://aws.amazon.com/ecr/pricing/
const std::string NET_DEV_FILE = "/proc/net/dev";

/*
 * This class estimates the AWS cost of each fargate container.
 */
class CostEstimation {
 private:
  struct CheckPointMetrics {
    double runtime;
    double networkRxBytes;
    double networkTxBytes;
    double cost;
    size_t peakRSS;
    size_t curRSS;

    folly::dynamic toDynamic() const {
      folly::dynamic res = folly::dynamic::object("runtime", runtime)(
          "networkRxBytes", networkRxBytes)("networkTxBytes", networkTxBytes)(
          "cost", cost)("peak mem", peakRSS)("current mem", curRSS);
      return res;
    }
  };
  std::string application_;
  std::string s3Bucket_;
  std::string s3Region_;
  std::string s3Path_;
  std::string version_; // example: decoupled, pcf2
  double estimatedCost_;
  int64_t runningTimeInSec_;
  long networkRXBytes_; // Network Receive bytes
  long networkTXBytes_; // Network Transmit bytes
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;
  size_t peakRSS_; // maximum virtual memory space used by the process, in kB
  std::unordered_map<std::string, CheckPointMetrics> checkPointMetrics_;
  std::vector<std::string> checkPointName_;
  int checkPoints_ = 0;
  void calculateCostCheckPoints();
  std::unordered_map<std::string, long> readNetworkSnapshot();
  size_t getPeakRSS();
  size_t getCurrentRSS();

 public:
  explicit CostEstimation(
      const std::string& app,
      const std::string& bucket,
      const std::string& region);
  explicit CostEstimation(
      const std::string& app,
      const std::string& bucket,
      const std::string& region,
      const std::string& version);

  std::string& getApplication();
  double& getEstimatedCost();
  long getNetworkBytes();
  void calculateCost();

  std::string getEstimatedCostString();
  folly::dynamic getEstimatedCostDynamic(
      std::string run_name,
      std::string party,
      folly::dynamic info);
  folly::dynamic getEstimatedCostDynamic(std::string run_name);

  void start();
  void end();
  void addCheckPoint(std::string checkPointName);

  std::string writeToS3(
      std::string party,
      std::string run_name,
      folly::dynamic costDynamic);
  std::string writeToS3(std::string run_name, folly::dynamic costDynamic);
  std::string _writeToS3(std::string filePath, folly::dynamic costDynamic);
};

} // namespace fbpcs::performance_tools
