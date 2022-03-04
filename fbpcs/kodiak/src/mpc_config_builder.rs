/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub struct MPCConfigBuilder {
    role: MPCRole,
    base_port: u16,
    concurrency: u32,
}

impl MPCConfigBuilder {
    pub fn new() -> Self {
        Self {
            role: MPCRole::Publisher,
            base_port: 8080,
            concurrency: 1,
        }
    }

    pub fn with_role(&mut self, role: MPCRole) -> Self {
        self.role = role;
        self
    }

    pub fn with_base_port(&mut self, base_port: u16) -> Self {
        self.base_port = base_port;
        self
    }

    pub fn with_concurrency(&mut self, concurrency: u32) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn build(&self) -> MPCConfig {
        MPCConfig {
            role: self.role,
            base_port: self.base_port,
            concurrency: self.concurrency,
        }
    }
}
