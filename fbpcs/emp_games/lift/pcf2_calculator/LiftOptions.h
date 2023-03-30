/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gflags/gflags_declare.h>
#include "fbpcs/emp_games/lift/common/CommonLiftOptions.h"

DECLARE_bool(is_conversion_lift);
DECLARE_string(input_directory);
DECLARE_string(input_expanded_key_path);
DECLARE_string(input_filenames);
DECLARE_string(input_global_params_path);
DECLARE_string(output_base_path);
DECLARE_string(output_directory);
DECLARE_string(output_filenames);
DECLARE_string(run_id);
