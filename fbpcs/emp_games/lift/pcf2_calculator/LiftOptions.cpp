/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "fbpcs/emp_games/lift/pcf2_calculator/LiftOptions.h"
#include <gflags/gflags.h>

DEFINE_bool(
    is_conversion_lift,
    true,
    "Use conversion_lift logic (as opposed to converter_lift logic)");
DEFINE_string(
    input_directory,
    "",
    "Data directory where input files are located");
DEFINE_string(
    input_expanded_key_path,
    "out.csv_expanded_key_0",
    "Input file name of the expanded key for UDP decryption. Used when decoupled UDP is enabled.");
DEFINE_string(
    input_filenames,
    "in.csv_0[,in.csv_1,in.csv_2,...]",
    "List of input file names that should be parsed (should have a header)");
DEFINE_string(
    input_global_params_path,
    "out.csv_global_params_0",
    "Input file name of global parameter setup. Used when reading inputs in secret share format rather than plaintext.");
DEFINE_string(
    output_base_path,
    "",
    "Local or s3 base path where output files are written to");
DEFINE_string(
    output_directory,
    "",
    "Local or s3 path where output files are written to");
DEFINE_string(
    output_filenames,
    "out.csv_0[,out.csv_1,out.csv_2,...]",
    "List of output file names that correspond to input filenames (positionally)");
DEFINE_string(
    run_id,
    "",
    "A run_id used to identify all the logs in a PL run.");
