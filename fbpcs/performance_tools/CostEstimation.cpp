/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/performance_tools/CostEstimation.h"
#include <fbpcf/io/api/FileIOWrappers.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fbpcs::performance_tools {

const std::unordered_map<std::string, std::string> SUPPORTED_APPLICATIONS(
    {{"data_processing", "dp-logs"},
     {"attributor", "att-logs"},
     {"aggregator", "agg-logs"},
     {"lift", "pl-logs"},
     {"shard_aggregator", "sa-logs"},
     {"shard_combiner", "sc-logs"},
     {"compactor", "comp-logs"}});
const std::vector<std::string> SUPPORTED_VERSIONS{"decoupled", "pcf2"};
const std::string CLOUD = "aws";

CostEstimation::CostEstimation(
    const std::string& app,
    const std::string& bucket,
    const std::string& region)
    : application_{app}, s3Bucket_{bucket}, s3Region_{region} {
  if (SUPPORTED_APPLICATIONS.find(app) == SUPPORTED_APPLICATIONS.end()) {
    XLOGF(ERR, "Application {} is not supported!", app);
  } else {
    s3Path_ = SUPPORTED_APPLICATIONS.at(app);
    version_ = "not_specified";
  }
}

CostEstimation::CostEstimation(
    const std::string& app,
    const std::string& bucket,
    const std::string& region,
    const std::string& version)
    : application_{app},
      s3Bucket_{bucket},
      s3Region_{region},
      version_{version} {
  if (SUPPORTED_APPLICATIONS.find(app) == SUPPORTED_APPLICATIONS.end()) {
    XLOGF(ERR, "Application {} is not supported!", app);
  } else {
    s3Path_ = SUPPORTED_APPLICATIONS.at(app);
  }
  if (std::find(
          SUPPORTED_VERSIONS.begin(), SUPPORTED_VERSIONS.end(), version) ==
      SUPPORTED_VERSIONS.end()) {
    XLOGF(ERR, "Version {} is not supported!", version);
  }
}

std::string& CostEstimation::getApplication() {
  return application_;
}

double& CostEstimation::getEstimatedCost() {
  return estimatedCost_;
}

long CostEstimation::getNetworkBytes() {
  return networkRXBytes_ + networkTXBytes_;
}

void CostEstimation::calculateCost() {
  // CPU cost
  double cpu_cost = vCPUS * (PER_CPU_HOUR_COST / 60) * (runningTimeInSec_ / 60);
  // Memory cost
  double memory_cost =
      MEMORY_SIZE * (PER_GB_HOUR_COST / 60) * (runningTimeInSec_ / 60);
  // Network cost
  double network_cost =
      (((networkRXBytes_ + networkTXBytes_) / 1024) / 1024 / 1024) *
      NETWORK_PER_GB_COST;
  // ECR cost
  double binarySizeInGB = 0.2; // The PA binary file is about ~200MB
  double ecr_cost = binarySizeInGB * ECR_PER_GB_COST;
  // Total estimated cost
  estimatedCost_ = cpu_cost + memory_cost + network_cost + ecr_cost;
}

std::unordered_map<std::string, long> CostEstimation::readNetworkSnapshot() {
  std::ifstream netdevfile{NET_DEV_FILE};
  std::unordered_map<std::string, long> result;
  result["rx"] = 0;
  result["tx"] = 0;
  for (std::string line; getline(netdevfile, line);) {
    if (line.find("eth0:") != std::string::npos ||
        line.find("eth1:") != std::string::npos) {
      std::vector<folly::StringPiece> pieces;
      folly::split(" ", line, pieces, true);
      result["rx"] += std::stoul(pieces.at(1).toString());
      result["tx"] += std::stoul(pieces.at(9).toString());
    } else {
      continue;
    }
  }
  return result;
}

std::string CostEstimation::getEstimatedCostString() {
  std::string result;
  result = "Running time: " + std::to_string(runningTimeInSec_) + "sec";
  result += "\nNetwork bytes(Rx+Tx): " +
      std::to_string(networkRXBytes_ + networkTXBytes_);
  result += "\nEstimated cost: $" + std::to_string(estimatedCost_);
  return result;
}

folly::dynamic CostEstimation::getEstimatedCostDynamic(
    std::string run_name,
    std::string party,
    folly::dynamic info) {
  folly::dynamic result = folly::dynamic::object;

  const auto now_ts = std::chrono::time_point_cast<std::chrono::seconds>(
      std::chrono::system_clock::now());
  const auto timestamp = now_ts.time_since_epoch().count();

  auto t = std::time(nullptr);
  char ds_string[40];
  struct tm newTime;
  std::strftime(
      ds_string, sizeof(ds_string), "%Y-%m-%d", localtime_r(&t, &newTime));

  result.insert("name", run_name);
  result.insert("party", party);
  result.insert("ds", ds_string);
  result.insert("timestamp", timestamp);
  result.insert("app_name", application_);
  result.insert("app_version", version_);
  result.insert("wall_time", runningTimeInSec_);
  result.insert("rx_bytes_dev", networkRXBytes_);
  result.insert("tx_bytes_dev", networkTXBytes_);
  result.insert("mem_alloted", MEMORY_SIZE);
  result.insert("cpu_alloted", vCPUS);
  result.insert("estimated_cost", estimatedCost_);
  result.insert("cloud_provider", CLOUD);
  result.insert("additional_info", folly::toJson(info));

  return result;
}

folly::dynamic CostEstimation::getEstimatedCostDynamic(std::string run_name) {
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

void CostEstimation::start() {
  start_time_ = std::chrono::system_clock::now();
  auto result = readNetworkSnapshot();
  if (!result.empty()) {
    networkRXBytes_ = result["rx"];
    networkTXBytes_ = result["tx"];
  }
}

void CostEstimation::end() {
  end_time_ = std::chrono::system_clock::now();
  auto result = readNetworkSnapshot();
  if (!result.empty()) {
    networkRXBytes_ = result["rx"] - networkRXBytes_;
    networkTXBytes_ = result["tx"] - networkTXBytes_;
  }

  runningTimeInSec_ = (end_time_ - start_time_) / std::chrono::seconds(1);
  calculateCost();
}

std::string CostEstimation::writeToS3(
    std::string party,
    std::string objectName,
    folly::dynamic costDynamic) {
  std::string s3FullPath =
      folly::to<std::string>("https://", s3Bucket_, s3Region_, s3Path_, "/");
  std::string filePath =
      folly::to<std::string>(s3FullPath, objectName, "_", party, ".json");

  return _writeToS3(filePath, costDynamic);
}

std::string CostEstimation::writeToS3(
    std::string objectName,
    folly::dynamic costDynamic) {
  std::string s3FullPath =
      folly::to<std::string>("https://", s3Bucket_, s3Region_, s3Path_, "/");
  std::string filePath =
      folly::to<std::string>(s3FullPath, objectName, ".json");

  return _writeToS3(filePath, costDynamic);
}

std::string CostEstimation::_writeToS3(
    std::string filePath,
    folly::dynamic costDynamic) {
  std::string costData = folly::toPrettyJson(costDynamic);
  try {
    XLOG(INFO) << "Writing cost file to s3: " << filePath;
    fbpcf::io::FileIOWrappers::writeFile(filePath, costData);
  } catch (const std::exception& e) {
    XLOG(WARN) << "Warning: Exception writing cost in S3.\n\terror msg: "
               << e.what();
    return "Failed to write " + filePath + ". Continuing execution.";
  }
  return "Successfully wrote cost info at : " + filePath;
}

} // namespace fbpcs::performance_tools
