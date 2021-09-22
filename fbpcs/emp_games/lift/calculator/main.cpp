/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <glog/logging.h>
#include <filesystem>
#include <sstream>
#include <string>

#include <gflags/gflags.h>

#include <folly/init/Init.h>
#include "folly/logging/xlog.h"

#include <fbpcf/aws/AwsSdk.h>
#include <fbpcf/mpc/MpcAppExecutor.h>
#include "CalculatorApp.h"

DEFINE_int32(party, 1, "1 = publisher, 2 = partner");
DEFINE_string(server_ip, "127.0.0.1", "Server's IP Address");
DEFINE_int32(
    port,
    15200,
    "Network port for establishing connection to other player");
DEFINE_string(
    input_directory,
    "",
    "Data directory where input files are located");
DEFINE_string(
    input_filenames,
    "in.csv_0[,in.csv_1,in.csv_2,...]",
    "List of input file names that should be parsed (should have a header)");
DEFINE_string(
    output_directory,
    "",
    "Local or s3 path where output files are written to");
DEFINE_string(
    output_filenames,
    "out.csv_0[,out.csv_1,out.csv_2,...]",
    "List of output file names that correspond to input filenames (positionally)");
DEFINE_string(
    input_base_path,
    "",
    "Local or s3 base path for the sharded input files");
DEFINE_string(
    output_base_path,
    "",
    "Local or s3 base path where output files are written to");
DEFINE_int32(
    file_start_index,
    0,
    "First file that will be read with base path");
DEFINE_int32(num_files, 0, "Number of files that should be read");
DEFINE_int64(
    epoch,
    1546300800,
    "Unixtime of 2019-01-01. Used as our 'new epoch' for timestamps");
DEFINE_bool(
    is_conversion_lift,
    true,
    "Use conversion_lift logic (as opposed to converter_lift logic)");
DEFINE_bool(
    use_xor_encryption,
    true,
    "Reveal output with XOR secret shares instead of in the clear to both parties");
DEFINE_int32(
    num_conversions_per_user,
    25,
    "Cap and pad to this many conversions per user");
DEFINE_int32(
    concurrency,
    1,
    "max number of game(s) that will run concurrently?");

using namespace private_lift;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  fbpcf::AwsSdk::aquire();
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<std::string> inputFilepaths;
  std::vector<std::string> outputFilepaths;

  if (!FLAGS_input_base_path.empty()) {
    std::string input_base_path = FLAGS_input_base_path + "_";
    std::string output_base_path = FLAGS_output_base_path + "_";
    for (auto i = FLAGS_file_start_index;
         i < FLAGS_file_start_index + FLAGS_num_files;
         ++i) {
      inputFilepaths.push_back(input_base_path + std::to_string(i));
      outputFilepaths.push_back(output_base_path + std::to_string(i));
    }
  } else {
    std::filesystem::path inputDirectory{FLAGS_input_directory};
    std::filesystem::path outputDirectory{FLAGS_output_directory};

    std::vector<std::string> inputFilenames;
    folly::split(',', FLAGS_input_filenames, inputFilenames);

    std::vector<std::string> outputFilenames;
    folly::split(",", FLAGS_output_filenames, outputFilenames);

    // Make sure the number of input files equals output files
    if (inputFilenames.size() != outputFilenames.size()) {
      XLOGF(
          ERR,
          "Error: input_filenames items ({}) does not equal output_filenames items ({})",
          inputFilenames.size(),
          outputFilenames.size());
      return 1;
    }

    for (auto i = 0; i < inputFilenames.size(); ++i) {
      inputFilepaths.push_back(inputDirectory / inputFilenames[i]);
      outputFilepaths.push_back(outputDirectory / outputFilenames[i]);
    }
  }

  {
    // Build a quick list of input/output files to log
    std::ostringstream inputFileLogList;
    for (auto inputFilepath : inputFilepaths) {
      inputFileLogList << "\t\t" << inputFilepath << "\n";
    }
    std::ostringstream outputFileLogList;
    for (auto outputFilepath : outputFilepaths) {
      outputFileLogList << "\t\t" << outputFilepath << "\n";
    }
    XLOG(INFO) << "Running conversion lift with settings:\n"
               << "\tparty: " << FLAGS_party << "\n"
               << "\tserver_ip_address: " << FLAGS_server_ip << "\n"
               << "\tport: " << FLAGS_port << "\n"
               << "\tconcurrency: " << FLAGS_concurrency << "\n"
               << "\tinput: " << inputFileLogList.str() << "\n"
               << "\toutput: " << outputFileLogList.str();
  }

  auto party = static_cast<fbpcf::Party>(FLAGS_party);

  // since DEFINE_INT16 is not supported, cast int32_t FLAGS_concurrency to
  // int16_t is necessarys here
  int16_t concurrency = static_cast<int16_t>(FLAGS_concurrency);

  // construct calculatorApps according to  FLAGS_num_shards and
  // FLAGS_concurrency
  std::vector<std::unique_ptr<CalculatorApp>> calculatorApps;
  for (auto i = 0; i < inputFilepaths.size(); i++) {
    calculatorApps.push_back(std::make_unique<CalculatorApp>(
        party,
        FLAGS_server_ip,
        FLAGS_port + i,
        inputFilepaths[i],
        outputFilepaths[i],
        FLAGS_use_xor_encryption));
  }

  // executor calculatorApps using fbpcf::MpcAppExecutor
  fbpcf::MpcAppExecutor<CalculatorApp> executor{concurrency};
  executor.execute(calculatorApps);

  return 0;
}
