/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use mpc_role::MPCRole;

struct NetworkConfigBuilder {
    role: MPCRole,
    host: String,
    port: u16,
}

impl NetworkConfig {
    pub fn new() -> Self {
        Self {
            role: MPCRole::Publisher,
            host: "localhost".to_string(),
            port: 8080,
        }
    }

    pub fn with_role(&mut self, role: MPCRole) -> Self {
        self.role = role;
        self
    }

    pub fn with_host(&mut self, host: String) -> Self {
        self.host = host;
        self
    }

    pub fn with_port(&mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn build(&self) -> NetworkConfig {
        NetworkConfig::new(self.role, self.host, self.port)
    }
}
