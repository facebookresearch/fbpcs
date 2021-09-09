/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


/*
 * Note: This is a duplicated file. It is copied here to enable
 * building of data processing code for one docker. It should be
 * deleted once OSS work figures out the correct directory structure.
 * Task: T93622832
*/
#pragma once

#include <cmath>
#include <ctime>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <chrono>
#include <fstream>
#include <folly/dynamic.h>
#include <folly/json.h>
#include "folly/logging/xlog.h"
#include <fbpcf/io/FileManagerUtil.h>


namespace measurement::private_attribution {

// Constants used for fargate container cost computation
const int64_t MEMORY_SIZE = 30;
const int64_t vCPUS = 4;
const double PER_CPU_HOUR_COST = 0.04656; // Source: https://aws.amazon.com/fargate/pricing/
const double PER_GB_HOUR_COST = 0.00511; // Source: https://aws.amazon.com/fargate/pricing/
const double NETWORK_PER_GB_COST = 0.01;
const double ECR_PER_GB_COST = 0.01;// Source: https://aws.amazon.com/ecr/pricing/


/*
 * This class estimates the AWS cost of each fargate container.
 */
class CostEstimation {
 public:
  explicit CostEstimation(const std::string& app) : application_{app} {
    s3Bucket_ = "run-logs-mpc";
    if (app == "attribution") {
      s3Path_ = "pa-logs";
    }
    else if (app == "data_processing") {
      s3Path_ = "dp-logs";
    }
    else if (app == "shard_aggregator") {
      s3Path_ = "sa-logs";
    }
  }

  const std::string& getApplication() const {
    return application_;
  }

  const double& getEstimatedCost() const {
    return estimatedCost_;
  }

  long getNetworkBytes() const {
    return networkRXBytes_ + networkTXBytes_;
  }

  void calculateCost(){
    // CPU cost
    double cpu_cost = vCPUS * (PER_CPU_HOUR_COST/60) * (runningTimeInSec_/60);
    // Memory cost
    double memory_cost = MEMORY_SIZE * (PER_GB_HOUR_COST/60) * (runningTimeInSec_/60);
    // Network cost
    double network_cost = (((networkRXBytes_ + networkTXBytes_) / 1024 ) / 1024 / 1024) * NETWORK_PER_GB_COST;
    // ECR cost
    double binarySizeInGB = 0.2; // The PA binary file is about ~200MB
    double ecr_cost = binarySizeInGB * ECR_PER_GB_COST;
    // Total estimated cost
    estimatedCost_ = cpu_cost + memory_cost + network_cost + ecr_cost;
  }

  std::unordered_map<std::string, long> readNetworkSnapshot(){
    std::ifstream netdevfile{"/proc/net/dev"};
    std::unordered_map<std::string, long> result;
    result["rx"] = 0;
    result["tx"] = 0;
    for (std::string line; getline(netdevfile, line); ) {
      if (line.find("eth0:") != std::string::npos || line.find("eth1:") != std::string::npos) {
        std::vector<folly::StringPiece> pieces;
        folly::split(" ", line, pieces, true);
        result["rx"] += std::stoul(pieces.at(1).toString());
        result["tx"] += std::stoul(pieces.at(9).toString());
      }
      else {
        continue;
      }
    }
    return result;
  }

  std::string getEstimatedCostString(){
    std::string result;
    result = "Running time: " + std::to_string(runningTimeInSec_) + "sec";
    result += "\nNetwork bytes(Rx+Tx): " + std::to_string(networkRXBytes_+networkTXBytes_);
    result += "\nEstimated cost: $" + std::to_string(estimatedCost_);
    return result;
  }

  folly::dynamic getEstimatedCostDynamic(
                        std::string run_name,
                        std::string attribution_rules,
                        std::string aggregators){
    folly::dynamic result = folly::dynamic::object;

    const auto now_ts = std::chrono::time_point_cast<std::chrono::seconds>(
                      std::chrono::system_clock::now());
    const auto timestamp = now_ts.time_since_epoch().count();
    result.insert("name", run_name);
    result.insert("timestamp", timestamp);
    result.insert("attribution_rule", attribution_rules);
    result.insert("aggregator", aggregators);
    result.insert("running_time", runningTimeInSec_);
    result.insert("rx_bytes", networkRXBytes_);
    result.insert("tx_bytes", networkTXBytes_);
    result.insert("estimated_cost", estimatedCost_);
    return result;
  }

  folly::dynamic getEstimatedCostDynamic(std::string run_name){
    folly::dynamic result = folly::dynamic::object;

    const auto now_ts = std::chrono::time_point_cast<std::chrono::seconds>(
                      std::chrono::system_clock::now());
    const auto timestamp = now_ts.time_since_epoch().count();
    result.insert("name", run_name);
    result.insert("timestamp", timestamp);
    result.insert("running_time", runningTimeInSec_);
    result.insert("rx_bytes", networkRXBytes_);
    result.insert("tx_bytes", networkTXBytes_);
    result.insert("estimated_cost", estimatedCost_);
    return result;
  }

  void start(){
    start_time_ = std::chrono::system_clock::now();
    auto result = readNetworkSnapshot();
    if (!result.empty()){
      networkRXBytes_ = result["rx"];
      networkTXBytes_ = result["tx"];
    }
  }

  void end(){
    end_time_ = std::chrono::system_clock::now();
    auto result = readNetworkSnapshot();
    if (!result.empty()){
      networkRXBytes_ = result["rx"] - networkRXBytes_;
      networkTXBytes_ = result["tx"] - networkTXBytes_;
    }

    runningTimeInSec_ = (end_time_ - start_time_) / std::chrono::seconds(1);
    calculateCost();
  }

  std::string writeToS3(std::string run_name, folly::dynamic costDynamic){

    std::string costData = folly::toPrettyJson(costDynamic);
    std::string s3FullPath = "https://" + s3Bucket_ + ".s3.us-west-2.amazonaws.com/" + s3Path_ + "/";
    std::string filePath = folly::to<std::string>(s3FullPath, run_name, ".json");

    try{
      fbpcf::io::write(filePath, costData);
    } catch (const std::exception& e) {
      XLOG(WARN) << "Error: Exception writing cost in S3.\n\terror msg: " << e.what();
      return "Failed to write " + filePath;
    }
    return "Successfully wrote cost info at : " + filePath;
  }

 private:
  std::string s3Bucket_;
  std::string s3Path_;
  std::string application_;
  double estimatedCost_;
  int64_t runningTimeInSec_;
  // TODO get cpu and mem usage
  long networkRXBytes_; // Network Receive bytes
  long networkTXBytes_; // Network Transmit bytes
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;
};

} // namespace measurement::private_attribution
