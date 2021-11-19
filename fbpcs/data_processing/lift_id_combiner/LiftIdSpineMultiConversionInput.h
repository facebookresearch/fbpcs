/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <iterator>
#include <map>
#include <sstream>
#include <string>

#include <gflags/gflags.h>

#include "LiftIdSpineCombinerOptions.h"

namespace pid {

enum class ConversionInputType { Valueless, WithValue };

class LiftIdSpineMultiConversionInput {
 public:
  LiftIdSpineMultiConversionInput() {}

  LiftIdSpineMultiConversionInput(uint64_t eventTimestamp, uint64_t value) {
    eventTimestampsToValues_.emplace(eventTimestamp, value);
  }

  // Keep adding at the end of values and event timestamp
  void update(uint64_t eventTimestamp, uint64_t value) {
    // Already have required number of elements
    if (eventTimestampsToValues_.size() ==
        static_cast<std::size_t>(FLAGS_multi_conversion_limit)) {
      return;
    }

    eventTimestampsToValues_.emplace(eventTimestamp, value);
  }

  // Processing complete for this event, stringify
  std::string toString(ConversionInputType ctype) {
    std::ostringstream output;

    while (eventTimestampsToValues_.size() <
           static_cast<std::size_t>(FLAGS_multi_conversion_limit)) {
      // Pad with 0 as needed
      eventTimestampsToValues_.emplace(0, 0);
    }

    std::ostringstream timestamps;
    std::ostringstream values;

    for (auto it = eventTimestampsToValues_.begin();
         it != eventTimestampsToValues_.end();
         ++it) {
      if (it != eventTimestampsToValues_.begin()) {
        timestamps << ",";
        values << ",";
      }
      timestamps << it->first;
      values << it->second;
    }
    output << '[' << timestamps.str() << ']';
    if (ctype == ConversionInputType::WithValue) {
      output << ',' << '[' << values.str() << ']';
    }
    return output.str();
  }

 private:
  std::multimap<uint64_t, uint64_t> eventTimestampsToValues_;
};
} // namespace pid
