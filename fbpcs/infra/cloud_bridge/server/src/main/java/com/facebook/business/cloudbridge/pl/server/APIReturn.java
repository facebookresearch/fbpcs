/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonValue;

class APIReturn {
  enum Status {
    STATUS_SUCCESS("success"),
    STATUS_FAIL("fail"),
    STATUS_ERROR("error");

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

  @JsonInclude(Include.NON_NULL)
  public Object data;

  public APIReturn(Status status, String message) {
    this.status = status;
    this.message = message;
    this.data = null;
  }

  public APIReturn(Status status, String message, Object data) {
    this.status = status;
    this.message = message;
    this.data = data;
  }
}
