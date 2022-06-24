/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <ctime>
#include <string>

namespace measurement::private_attribution {

/*
 * This chunk size has to be large enough that we don't make
 * unnecessary trips to cloud storage but small enough that
 * we don't cause OOM issues. This chunk size was chosen based
 * on the size of our containers as well as the expected size
 * of our files to fit the aforementioned constraints.
 */
constexpr size_t kBufferedReaderChunkSize = 1073741824; // 2^30

inline std::string getDateString() {
  time_t timeNow = time(nullptr);
  char dateStr[12];
  struct tm newTime;
  strftime(dateStr, sizeof(dateStr), "%Y-%m-%d", gmtime_r(&timeNow, &newTime));
  return dateStr;
}

} // namespace measurement::private_attribution
