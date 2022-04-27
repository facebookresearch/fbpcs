/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gflags/gflags.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <folly/init/Init.h>

#include "fbpcs/emp_games/common/Csv.h"
#include "fbpcs/emp_games/lift/calculator/InputData.h"
#include "fbpcs/emp_games/lift/calculator/OutputMetrics.h"
#include "fbpcs/emp_games/lift/calculator/test/common/LiftCalculator.h"

DEFINE_string(
    input_directory,
    "sample_input",
    "Data directory where input files are located");
DEFINE_string(
    input_publisher_filename,
    "publisher_0",
    "Input file name that should be parsed (should have a header)");
DEFINE_string(
    input_partner_filename,
    "partner_4_convs_0",
    "Input file name that should be parsed (should have a header)");
DEFINE_string(output_directory, "", "Path where output files are written to");
DEFINE_string(
    output_filename,
    "out.csv",
    "Output file name that correspond to input filenames (positionally)");

using namespace private_lift;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string publisherInputFilepath;
  std::string partnerInputFilepath;
  std::string outputFilepath;

  publisherInputFilepath =
      FLAGS_input_directory + "/" + FLAGS_input_publisher_filename;
  partnerInputFilepath =
      FLAGS_input_directory + "/" + FLAGS_input_partner_filename;
  outputFilepath = FLAGS_output_directory + "/" + FLAGS_output_filename;

  // TODO: enable random data generation with provided desired data size.

  // Start measuring time
  struct timeval begin, end;
  gettimeofday(&begin, 0);

  LiftCalculator liftCalculator;
  std::ifstream inFilePublisher{publisherInputFilepath};
  if (!inFilePublisher.is_open()) {
    std::cout << "failed to open '" << publisherInputFilepath << "'"
              << std::endl;
    return 1;
  }
  std::ifstream inFilePartner{partnerInputFilepath};
  if (!inFilePartner.is_open()) {
    std::cout << "failed to open '" << partnerInputFilepath << "'" << std::endl;
    return 1;
  }
  int32_t tsOffset = 10;
  std::string linePublisher;
  std::string linePartner;
  getline(inFilePublisher, linePublisher);
  getline(inFilePartner, linePartner);
  auto headerPublisher =
      private_measurement::csv::splitByComma(linePublisher, false);
  auto headerPartner =
      private_measurement::csv::splitByComma(linePartner, false);
  std::unordered_map<std::string, int> colNameToIndex =
      liftCalculator.mapColToIndex(headerPublisher, headerPartner);
  OutputMetricsData computedResult = liftCalculator.compute(
      inFilePublisher, inFilePartner, colNameToIndex, tsOffset);

  gettimeofday(&end, 0);
  long seconds = end.tv_sec - begin.tv_sec;
  long microseconds = end.tv_usec - begin.tv_usec;
  double elapsed = seconds * 1e+3 + microseconds * 1e-3;

  std::cout << "start time: " << ctime((const time_t*)&begin.tv_sec)
            << "time used (ms): " << elapsed << "\n"
            << computedResult << std::endl;

  // write output to file

  std::ofstream file(outputFilepath);
  if (file.is_open()) {
    file << "start time: " << ctime((const time_t*)&begin.tv_sec);
    file << "time used (ms): " << elapsed << "\n" << computedResult;
    file.close();
  } else {
    std::cout << "Cannot open out file: " << outputFilepath << std::endl;
  }
  return 0;
}
