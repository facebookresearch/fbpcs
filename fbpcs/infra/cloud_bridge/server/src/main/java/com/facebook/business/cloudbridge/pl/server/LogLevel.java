/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

enum LogLevel {
  DISABLED("Disabled"),
  ERROR("ERROR"),
  WARNING("WARN"),
  INFORMATION("INFO"),
  DEBUG("DEBUG"),
  TRACE("TRACE");

  private final String level;

  LogLevel() {
    this.level = "DEBUG";
  }

  LogLevel(final String level) {
    this.level = level;
  }

  public String getLevel() {
    return this.level;
  }

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static LogLevel getLogLevelFromName(@JsonProperty("logLevel") String level) {
    for (LogLevel l : LogLevel.values()) {
      if (l.getLevel().equals(level)) {
        return l;
      }
    }
    return DEBUG;
  }
}
