/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::mpc_role::MPCRole;

pub struct NetworkConfig {
    role: MPCRole,
    host: String,
    port: u16,
}

impl NetworkConfig {
    pub fn new(role: MPCRole, host: String, port: u16) -> Self {
        Self { role, host, port }
    }
}
