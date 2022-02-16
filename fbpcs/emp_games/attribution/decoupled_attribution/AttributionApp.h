/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include <fbpcf/io/FileManagerUtil.h>
#include <fbpcf/mpc/EmpApp.h>
#include <fbpcf/mpc/EmpGame.h>

#include "fbpcs/emp_games/attribution/decoupled_attribution/Attribution.hpp"

namespace aggregation::private_attribution {

template <int MY_ROLE, fbpcf::Visibility OUTPUT_VISIBILITY, class IOChannel>
class AttributionGame : public fbpcf::EmpGame<
                            IOChannel,
                            AttributionInputMetrics,
                            AttributionOutputMetrics> {
 public:
  AttributionGame(std::unique_ptr<IOChannel> ioChannel, fbpcf::Party party)
      : fbpcf::EmpGame<
            IOChannel,
            AttributionInputMetrics,
            AttributionOutputMetrics>(std::move(ioChannel), party) {}

  AttributionOutputMetrics play(
      const AttributionInputMetrics& inputData) override {
    XLOG(INFO, "Running attribution");
    const auto out = computeAttributions<MY_ROLE>(inputData, OUTPUT_VISIBILITY);
    XLOG(INFO, "Attribution completed.");
    return out;
  }
};

template <int MY_ROLE, fbpcf::Visibility OUTPUT_VISIBILITY>
class AttributionApp
    : public fbpcf::EmpApp<
          AttributionGame<MY_ROLE, OUTPUT_VISIBILITY, emp::NetIO>,
          AttributionInputMetrics,
          AttributionOutputMetrics> {
 public:
  AttributionApp(
      const std::string& serverIp,
      const uint16_t& port,
      const std::string& attributionRules,
      const std::string& inputPath,
      const std::string& outputPath,
      const bool useTls,
      const std::string& tlsDir)
      : fbpcf::EmpApp<
            AttributionGame<MY_ROLE, OUTPUT_VISIBILITY, emp::NetIO>,
            AttributionInputMetrics,
            AttributionOutputMetrics>{static_cast<fbpcf::Party>(MY_ROLE), serverIp, port, useTls, tlsDir},
        attributionRules_{attributionRules},
        inputPath_{inputPath},
        outputPath_{outputPath} {}

 protected:
  AttributionInputMetrics getInputData() override {
    XLOG(INFO) << "MY_ROLE: " << MY_ROLE
               << ", attributionRules_: " << attributionRules_
               << ", input_path: " << inputPath_;
    return AttributionInputMetrics{MY_ROLE, attributionRules_, inputPath_};
  }

  void putOutputData(const AttributionOutputMetrics& attributions) override {
    fbpcf::io::write(outputPath_, attributions.toJson());
  }

 private:
  std::string attributionRules_;
  std::string inputPath_;
  std::string outputPath_;
};
} // namespace aggregation::private_attribution
