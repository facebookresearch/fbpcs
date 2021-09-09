/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <string>

#include <gflags/gflags.h>

#include "folly/logging/xlog.h"

#include "../common/GenFakeData.h"

DEFINE_int32(role, 1, "1 = publisher, 2 = partner");
DEFINE_string(
    output_path,
    "lift_input",
    "directory and filename of the generated synthetic lift input file");
DEFINE_int32(num_shards, 1, "number of shards to be generated");
DEFINE_int32(num_rows, 10, "number of rows of data to be generated");
DEFINE_double(opportunity_rate, 0.5, "opportunity rate");
DEFINE_double(test_rate, 0.5, "test rate");
DEFINE_double(purchase_rate, 0.5, "purchase rate");
DEFINE_double(incrementality_rate, 0.0, "incrementality rate");
DEFINE_int64(
    epoch,
    1546300800,
    "Unixtime of 2019-01-01. Used as our 'new epoch' for timestamps");
DEFINE_int32(
    num_conversions_per_user,
    4,
    "Cap and pad to this many conversions per user");
DEFINE_bool(
    omit_values_column,
    false,
    "Omit values column from partner's dataset");

int main(int argc, char** argv) {
  // Parse input
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  XLOG(INFO) << "Generating " << FLAGS_num_shards
             << " file(s): with input values: ";
  XLOG(INFO) << "\trole: " << FLAGS_role;
  XLOG(INFO) << "\tnum rows: " << FLAGS_num_rows;
  XLOG(INFO) << "\topportunity rate: " << FLAGS_opportunity_rate;
  XLOG(INFO) << "\ttest rate: " << FLAGS_test_rate;
  XLOG(INFO) << "\tpurchase rate: " << FLAGS_purchase_rate;
  XLOG(INFO) << "\tincrementatlity rate: " << FLAGS_incrementality_rate;
  XLOG(INFO) << "\tepoch: " << FLAGS_epoch;

  private_lift::GenFakeData fakeFileGenerator;
  // generate synthetic input file with givn flags

  if (FLAGS_role == 1) {
    for (auto i = 0; i < FLAGS_num_shards; i++) {
      auto outputFile = FLAGS_output_path + "_" + std::to_string(i);
      XLOG(INFO) << "Generating " << std::to_string(i)
                 << "th file: " << outputFile;

      private_lift::LiftFakeDataParams params;
      params.setNumRows(FLAGS_num_rows)
          .setOpportunityRate(FLAGS_opportunity_rate)
          .setTestRate(FLAGS_test_rate)
          .setPurchaseRate(FLAGS_purchase_rate)
          .setIncrementalityRate(FLAGS_incrementality_rate)
          .setEpoch(FLAGS_epoch);
      fakeFileGenerator.genFakePublisherInputFile(outputFile, params);
    }
  } else if (FLAGS_role == 2) {
    XLOG(INFO) << "\tnum conversions per user: "
               << FLAGS_num_conversions_per_user;
    for (auto i = 0; i < FLAGS_num_shards; i++) {
      auto outputFile = FLAGS_output_path + "_" + std::to_string(i);
      XLOG(INFO) << "Generating " << std::to_string(i)
                 << "th file: " << outputFile;

      private_lift::LiftFakeDataParams params;
      params.setNumRows(FLAGS_num_rows)
          .setOpportunityRate(FLAGS_opportunity_rate)
          .setTestRate(FLAGS_test_rate)
          .setPurchaseRate(FLAGS_purchase_rate)
          .setIncrementalityRate(FLAGS_incrementality_rate)
          .setEpoch(FLAGS_epoch)
          .setNumConversions(FLAGS_num_conversions_per_user)
          .setOmitValuesColumn(FLAGS_omit_values_column);
      fakeFileGenerator.genFakePartnerInputFile(outputFile, params);
    }
  } else {
    throw std::invalid_argument(
        "Value of arugment role should be 1 or 2: 1 = publisher, 2 = partner.");
  }
  XLOG(INFO) << "Finished generating " << FLAGS_num_shards << " file(s).";

  return 0;
}
