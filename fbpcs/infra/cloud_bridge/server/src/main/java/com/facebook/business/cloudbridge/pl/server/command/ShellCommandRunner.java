/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server.command;

import java.util.List;
import java.util.Map;

public interface ShellCommandRunner {
  class CommandRunnerResult {
    Integer exitCode;

    public Integer getExitCode() {
      return this.exitCode;
    }

    CommandRunnerResult(Integer exitCode) {
      this.exitCode = exitCode;
    }
  }

  public CommandRunnerResult run(
      final List<String> commandToRun,
      final Map<String, String> commandEnvironment,
      String commandOutputDirectoryPath,
      String commandOutputFilePath);
}
