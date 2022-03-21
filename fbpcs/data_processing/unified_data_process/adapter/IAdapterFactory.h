/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include "fbpcs/data_processing/unified_data_process/adapter/IAdapter.h"

namespace unified_data_process::adapter {

class IAdapterFactory {
 public:
  virtual ~IAdapterFactory() = default;
  virtual std::unique_ptr<IAdapter> create() = 0;
};

} // namespace unified_data_process::adapter
