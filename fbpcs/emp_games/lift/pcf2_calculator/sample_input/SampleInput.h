/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "tools/cxx/Resources.h"

namespace private_lift::sample_input {

inline boost::filesystem::path getPublisherInput1() {
  return build::getResourcePath(
      "fbpcs/emp_games/lift/pcf2_calculator/sample_input/publisher_unittest.csv");
}

inline boost::filesystem::path getPublisherInput2() {
  return build::getResourcePath(
      "fbpcs/emp_games/lift/pcf2_calculator/sample_input/publisher_unittest2.csv");
}

inline boost::filesystem::path getPublisherInput3() {
  return build::getResourcePath(
      "fbpcs/emp_games/lift/pcf2_calculator/sample_input/publisher_unittest3.csv");
}

inline boost::filesystem::path getPartnerInput2() {
  return build::getResourcePath(
      "fbpcs/emp_games/lift/pcf2_calculator/sample_input/partner_2_convs_unittest.csv");
}

inline boost::filesystem::path getPartnerInput4() {
  return build::getResourcePath(
      "fbpcs/emp_games/lift/pcf2_calculator/sample_input/partner_4_convs_unittest.csv");
}

inline boost::filesystem::path getPartnerConverterInput() {
  return build::getResourcePath(
      "fbpcs/emp_games/lift/pcf2_calculator/sample_input/partner_converter_unittest.csv");
}

// For publisher3 and partner2 inputs
inline boost::filesystem::path getCorrectnessOutput() {
  return build::getResourcePath(
      "fbpcs/emp_games/lift/pcf2_calculator/sample_input/correctness_output.json");
}

} // namespace private_lift::sample_input
