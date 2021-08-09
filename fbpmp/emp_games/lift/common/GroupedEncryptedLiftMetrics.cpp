/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "GroupedEncryptedLiftMetrics.h"

#include <vector>

#include <fbpcf/common/FunctionalUtil.h>
#include <fbpcf/common/VectorUtil.h>

namespace private_lift {

GroupedEncryptedLiftMetrics GroupedEncryptedLiftMetrics::operator+(
    const GroupedEncryptedLiftMetrics& other) const noexcept {
  return GroupedEncryptedLiftMetrics{
      metrics + other.metrics,
      fbpcf::vector::Add(cohortMetrics, other.cohortMetrics)};
}

GroupedEncryptedLiftMetrics GroupedEncryptedLiftMetrics::operator^(
    const GroupedEncryptedLiftMetrics& other) const noexcept {
  return GroupedEncryptedLiftMetrics{
      metrics ^ other.metrics,
      fbpcf::vector::Xor(cohortMetrics, other.cohortMetrics)};
}
} // namespace private_lift
