/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>

#include "fbpcf/test/TestHelper.h"
#include "fbpcs/emp_games/common/Constants.h"

namespace common {

using SchedulerType = fbpcf::SchedulerType;

inline std::string getInputEncryptionString(
    common::InputEncryption inputEncryption) {
  switch (inputEncryption) {
    case common::InputEncryption::Plaintext:
      return "Plaintext";
    case common::InputEncryption::PartnerXor:
      return "PartnerXor";
    case common::InputEncryption::Xor:
      return "Xor";
  }
}

inline std::string getVisibilityString(common::Visibility visibility) {
  switch (visibility) {
    case common::Visibility::Xor:
      return "Xor";
    case common::Visibility::Publisher:
      return "Publisher";
  }
}
} // namespace common
