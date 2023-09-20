/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server.deployment.models;

public class DeploymentMeta {
  private String deploymentId;
  private DeploymentStatus deploymentStatus;

  public DeploymentMeta(final String deploymentId, final DeploymentStatus deploymentStatus) {
    this.deploymentId = deploymentId;
    this.deploymentStatus = deploymentStatus;
  }

  public String getDeploymentId() {
    return this.deploymentId;
  }

  public DeploymentStatus getDeploymentStatus() {
    return this.deploymentStatus;
  }

  public void setDeploymentId(String deploymentId) {
    this.deploymentId = deploymentId;
  }

  public void setDeploymentStatus(DeploymentStatus deploymentStatus) {
    this.deploymentStatus = deploymentStatus;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof DeploymentMeta)) return false;
    DeploymentMeta other = (DeploymentMeta) o;
    Object this$deploymentId = this.getDeploymentId();
    Object other$deploymentId = other.getDeploymentId();
    if (this$deploymentId == null
        ? other$deploymentId != null
        : !this$deploymentId.equals(other$deploymentId)) return false;
    Object this$deploymentStatus = this.getDeploymentStatus();
    Object other$deploymentStatus = other.getDeploymentStatus();
    if (this$deploymentStatus == null
        ? other$deploymentStatus != null
        : !this$deploymentStatus.equals(other$deploymentStatus)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    int PRIME = 59;
    int result = 1;
    Object $deploymentId = this.getDeploymentId();
    result = result * PRIME + ($deploymentId == null ? 43 : $deploymentId.hashCode());
    Object $deploymentStatus = this.getDeploymentStatus();
    result = result * PRIME + ($deploymentStatus == null ? 43 : $deploymentStatus.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "DeploymentMeta(deploymentId="
        + getDeploymentId()
        + ", deploymentStatus="
        + getDeploymentStatus()
        + ")";
  }
}
