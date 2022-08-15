/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <filesystem>

#include <gflags/gflags.h>
#include <signal.h>

#include "folly/init/Init.h"

// TODO: Rewrite for OSS?
#include "fbpcf/aws/AwsSdk.h"

#include "LiftIdSpineCombinerOptions.h"
#include "LiftIdSpineFileCombiner.h"

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  fbpcf::AwsSdk::aquire();
  signal(SIGPIPE, SIG_IGN);

  pid::combiner::combineFile(
      FLAGS_data_path,
      FLAGS_spine_path,
      FLAGS_output_path,
      FLAGS_tmp_directory,
      FLAGS_sort_strategy,
      FLAGS_max_id_column_cnt,
      FLAGS_protocol_type);

  return 0;
}
