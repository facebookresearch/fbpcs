/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fbpcf/mpc_std_lib/oram/encoder/IFilter.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace pc_translator {

using IFilter = fbpcf::mpc_std_lib::oram::IFilter;
/*
 * Class to store each filter constraint include in the PC instruction set.
 */
class FilterConstraint {
 public:
  FilterConstraint(
      const std::string& name,
      const IFilter::FilterType type,
      int value);

  /*
   * Name of the filter constraint i.e. the field on which this filter is to be
   * applied.
   */
  std::string getName() const;

  /*
   * Constraint type i.e. LT, LTE, EQ, NEQ etc.
   */
  IFilter::FilterType getType() const;

  int getValue() const;

 private:
  std::string name_;
  IFilter::FilterType type_;
  int value_;
};

} // namespace pc_translator
