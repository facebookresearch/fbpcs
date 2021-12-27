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

#include "Attribution.hpp"

namespace measurement::private_attribution {

template <int MY_ROLE, fbpcf::Visibility OUTPUT_VISIBILITY>
class AttributionGame : public fbpcf::EmpGame<
                            emp::NetIO,
                            AttributionInputMetrics,
                            AttributionOutputMetrics> {
 public:
  AttributionGame(std::unique_ptr<emp::NetIO> ioChannel, fbpcf::Party party)
      : fbpcf::EmpGame<
            emp::NetIO,
            AttributionInputMetrics,
            AttributionOutputMetrics>(std::move(ioChannel), party) {}

  AttributionOutputMetrics play(
      const AttributionInputMetrics& inputData) override {
    XLOG(INFO, "Running attribution");
    const auto out = computeAttributions<MY_ROLE>(inputData, OUTPUT_VISIBILITY);
    XLOGF(INFO, "Done. Output: {}", folly::toPrettyJson(out.toDynamic()));
    return out;
  }
};

template <int MY_ROLE, fbpcf::Visibility OUTPUT_VISIBILITY>
class AttributionApp : public fbpcf::EmpApp<
                           AttributionGame<MY_ROLE, OUTPUT_VISIBILITY>,
                           AttributionInputMetrics,
                           AttributionOutputMetrics> {
 public:
  AttributionApp(
      const std::string& serverIp,
      const uint16_t& port,
      const std::string& attributionRules,
      const std::string& aggregators,
      const std::string& inputPath,
      const std::string& outputPath)
      : fbpcf::EmpApp<
            AttributionGame<MY_ROLE, OUTPUT_VISIBILITY>,
            AttributionInputMetrics,
            AttributionOutputMetrics>{static_cast<fbpcf::Party>(MY_ROLE), serverIp, port},
        attributionRules_{attributionRules},
        aggregators_{aggregators},
        inputPath_{inputPath},
        outputPath_{outputPath} {}

 protected:
  AttributionInputMetrics getInputData() override {
    XLOG(INFO) << "MY_ROLE: " << MY_ROLE
               << ", attributionRules_: " << attributionRules_
               << ", aggregators_: " << aggregators_
               << ", input_path: " << inputPath_;
    return AttributionInputMetrics{
        MY_ROLE, attributionRules_, aggregators_, inputPath_};
  }

  void putOutputData(const AttributionOutputMetrics& attributions) override {
    fbpcf::io::write(outputPath_, attributions.toJson());
  }

 private:
  std::string attributionRules_;
  std::string aggregators_;
  std::string inputPath_;
  std::string outputPath_;
};
} // namespace measurement::private_attribution
