/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.fasterxml.jackson.annotation.JsonValue;

public class PCEValidatorAPIReturn {
  enum Status {
    STATUS_SUCCESS("success"),
    STATUS_FAIL("fail");
    private String status;

    Status(String status) {
      this.status = status;
    }

    @JsonValue
    public String getStatus() {
      return status;
    }
  }

  public Status status;
  public String message;

  public PCEValidatorAPIReturn(Status status, String message) {
    this.status = status;
    this.message = message;
  }
}
