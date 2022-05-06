/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
namespace common {

const int PUBLISHER = 0;
const int PARTNER = 1;

enum class Visibility { Publisher, Xor };

enum class InputEncryption {
  Plaintext, // inputs are all plaintext
  PartnerXor, // partner input is XOR secret shared
  Xor // both publisher and partner inputs are XOR secret shared
};

/*
  ATTRIBUTION RULE NAMES
*/
inline const std::string LAST_CLICK_1D = "last_click_1d";
inline const std::string LAST_TOUCH_1D = "last_touch_1d";
inline const std::string LAST_CLICK_28D = "last_click_28d";
inline const std::string LAST_TOUCH_28D = "last_touch_28d";
inline const std::string LAST_CLICK_2_7D = "last_click_2_7d";
inline const std::string LAST_TOUCH_2_7D = "last_touch_2_7d";
inline const std::string LAST_CLICK_1D_TARGETID = "last_click_1d_targetid";

/*
  ADDITIONAL INPUT COLUMNS
*/
inline const std::string TARGET_ID = "targetid";
inline const std::string TARGET_ID_ACTION_TYPE = "targetid_actiontype";

/*
  AGGREGATOR NAMES
*/
inline const std::string MEASUREMENT = "measurement";

} // namespace common
