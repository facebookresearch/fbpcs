/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <ctime>
#include <string>

namespace measurement::private_attribution {

inline std::string getDateString() {
  time_t timeNow = time(nullptr);
  char dateStr[12];
  struct tm newTime;
  strftime(dateStr, sizeof(dateStr), "%Y-%m-%d", gmtime_r(&timeNow, &newTime));
  return dateStr;
}

} // namespace measurement::private_attribution
