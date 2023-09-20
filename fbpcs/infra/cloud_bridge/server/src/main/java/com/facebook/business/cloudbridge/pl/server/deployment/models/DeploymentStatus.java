/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server.deployment.models;

public enum DeploymentStatus {
  NOT_STARTED("NOT_STARTED"),
  STARTED("STARTED"),
  ERROR("ERROR"),
  COMPLETED("COMPLETED"),
  ;

  private final String status;

  private DeploymentStatus(final String status) {
    this.status = status;
  }

  public String getStatus() {
    return this.status;
  }
}
