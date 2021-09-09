/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fstream>
#include <iomanip>
#include <unordered_map>

#include <gflags/gflags.h>

#include "folly/logging/xlog.h"

#include "../../../../common/Csv.h"
#include "../../OutputMetricsData.h"
#include "../common/LiftCalculator.h"

DEFINE_string(
    publisher,
    "in_publisher.csv",
    "Name of the input file from publisher");
DEFINE_string(partner, "in_partner.csv", "Name of the input file from partner");
DEFINE_int32(
    tsoffset,
    10,
    "timestamp offset to be added to event timestamp before comparing to opportunity timestamp");

int main(int argc, char** argv) {
  // Parse input
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto publisherInputFile = FLAGS_publisher;
  auto partnerInputFile = FLAGS_partner;
  auto tsOffset = FLAGS_tsoffset;

  std::ifstream inFilePublisher{publisherInputFile};
  std::ifstream inFilePartner{partnerInputFile};

  if (!inFilePublisher.good()) {
    XLOG(FATAL) << "Failed to read input file " << publisherInputFile;
  }
  if (!inFilePartner.good()) {
    XLOG(FATAL) << "Failed to read input file " << partnerInputFile;
  }

  // Check header
  std::string linePublisher;
  std::string linePartner;
  getline(inFilePublisher, linePublisher);
  getline(inFilePartner, linePartner);
  auto headerPublisher =
      private_measurement::csv::splitByComma(linePublisher, false);
  auto headerPartner =
      private_measurement::csv::splitByComma(linePartner, false);

  private_lift::LiftCalculator liftCalculator;

  std::unordered_map<std::string, int> colNameToIndex =
      liftCalculator.mapColToIndex(headerPublisher, headerPartner);

  // Calculate
  private_lift::OutputMetricsData out = liftCalculator.compute(
      inFilePublisher, inFilePartner, colNameToIndex, tsOffset);

  // Output results
  XLOG(INFO) << std::setw(20) << "test_population: " << std::setw(12)
             << out.testPopulation;
  XLOG(INFO) << std::setw(20) << "control_population: " << std::setw(12)
             << out.controlPopulation;
  XLOG(INFO) << std::setw(20) << "test_event: " << std::setw(12)
             << out.testEvents;
  XLOG(INFO) << std::setw(20) << "control_event: " << std::setw(12)
             << out.controlEvents;
  XLOG(INFO) << std::setw(20) << "test_value: " << std::setw(12)
             << out.testValue;
  XLOG(INFO) << std::setw(20) << "control_value: " << std::setw(12)
             << out.controlValue;
  XLOG(INFO) << std::setw(20) << "test_value_sq: " << std::setw(12)
             << out.testValueSquared;
  XLOG(INFO) << std::setw(20) << "control_value_sq: " << std::setw(12)
             << out.controlValueSquared;
  XLOG(INFO) << std::setw(20) << "test_num_conv_sq: " << std::setw(12)
             << out.testNumConvSquared;
  XLOG(INFO) << std::setw(20) << "control_num_conv_sq: " << std::setw(12)
             << out.controlNumConvSquared;

  return 0;
}
