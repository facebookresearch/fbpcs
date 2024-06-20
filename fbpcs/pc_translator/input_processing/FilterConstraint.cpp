/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/pc_translator/input_processing/FilterConstraint.h"
#include <fbpcf/mpc_std_lib/oram/encoder/IFilter.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

using fbpcf::mpc_std_lib::oram::IFilter;
namespace pc_translator {

using IFilter = fbpcf::mpc_std_lib::oram::IFilter;

FilterConstraint::FilterConstraint(
    const std::string& name,
    const IFilter::FilterType type,
    int value)
    : name_(name), type_(type), value_(value) {}

std::string FilterConstraint::getName() const {
  return name_;
}

IFilter::FilterType FilterConstraint::getType() const {
  return type_;
}

int FilterConstraint::getValue() const {
  return value_;
}
} // namespace pc_translator
