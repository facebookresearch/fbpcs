/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "fbpcs/data_processing/load_testing_utils/FakeDataGenerator.h"

DEFINE_string(output_filepath, "", "Path of the output file");
DEFINE_string(
    role,
    "publisher",
    "Whether this is a publisher or partner dataset");
DEFINE_string(header, "", "Header defining the output to be generated");
DEFINE_int32(n, 1'000'000, "How many lines to generate");
DEFINE_int32(log_every_n, 1'000'000, "How frequently to log updates");
DEFINE_string(
    opportunity_rate,
    "0.8",
    "Rate of logged opportunities (as a double)");
DEFINE_string(
    test_rate,
    "0.9",
    "Proportion of opportunities logged to test group (as a double)");
DEFINE_string(
    purchase_rate,
    "0.1",
    "Proportion of users making a purchase (as a double)");
DEFINE_int32(min_ts, 1'600'000'000, "Minimum timestamp possible");
DEFINE_int32(max_ts, 1'600'000'000 + 86400 * 30, "Maximum timestamp possible");
DEFINE_int32(min_value, 100, "Minimum value for generated purchases");
DEFINE_int32(max_value, 10'000, "Maximum value for generated purchases");
DEFINE_bool(
    should_use_complex_ids,
    true,
    "Use complex IDs instead of simple integers");

static Role parseRole(std::string role) {
  std::transform(role.begin(), role.end(), role.begin(), [](unsigned char c) {
    return std::tolower(c);
  });
  return role == "publisher" ? Role::Publisher : Role::Partner;
}

static std::vector<std::string> parseHeader(std::string header) {
  std::vector<std::string> res;
  std::size_t start = 0;
  std::size_t i = 0;
  while (start + i < header.size()) {
    if (header.at(start + i) == ',') {
      res.push_back(header.substr(start, i));
      start += i + 1;
      i = 0;
    } else {
      ++i;
    }
  }
  res.push_back(header.substr(start));
  return res;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto role = parseRole(FLAGS_role);
  auto header = parseHeader(FLAGS_header);

  FakeDataGeneratorParams params{role, header};

  params = params.withOpportunityRate(std::stod(FLAGS_opportunity_rate))
               .withTestRate(std::stod(FLAGS_test_rate))
               .withPurchaseRate(std::stod(FLAGS_purchase_rate))
               .withMinTs(FLAGS_min_ts)
               .withMaxTs(FLAGS_max_ts)
               .withMinValue(FLAGS_min_value)
               .withMaxValue(FLAGS_max_value)
               .withShouldUseComplexIds(FLAGS_should_use_complex_ids);

  FakeDataGenerator g{params};

  std::cout << "Writing output to " << FLAGS_output_filepath << '\n';
  std::ofstream fOut{FLAGS_output_filepath};
  fOut << FLAGS_header << '\n';
  for (std::size_t i = 0; i < FLAGS_n; ++i) {
    auto row = g.genOneRow();
    if (!row.empty()) {
      fOut << row << '\n';
    }
    if ((i + 1) % FLAGS_log_every_n == 0) {
      std::cout << "Processed " << i + 1 << " lines\n";
    }
  }

  std::cout << "Done.\n";
  return 0;
}
