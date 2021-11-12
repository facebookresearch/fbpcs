/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/common/SecretSharing.h"

#include <emp-sh2pc/emp-sh2pc.h>

#include <fbpcf/mpc/EmpGame.h>

namespace private_measurement::secret_sharing {
emp::Bit privatelyShare(fbpcf::Party dataSrc, bool data) {
  // Unfortunately this ugly static_cast is necessary for the EMP library
  emp::Bit res{data, static_cast<int>(dataSrc)};
  return res;
}

emp::Integer privatelyShare(fbpcf::Party dataSrc, int64_t data) {
  // Unfortunately this ugly static_cast is necessary for the EMP library
  emp::Integer res{INT_SIZE, data, static_cast<int>(dataSrc)};
  return res;
}
} // namespace private_measurement::secret_sharing
