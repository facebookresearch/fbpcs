/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server;

public class InvalidDeploymentArgumentException extends Exception {
  public InvalidDeploymentArgumentException(String message) {
    super(message);
  }

  private static final long serialVersionUID = 855L;
}
