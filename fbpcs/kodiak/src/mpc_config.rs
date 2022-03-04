/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub struct MPCConfig {
    role: MPCRole,
    base_port: u16,
    concurrency: u32,
}

impl MPCConfig {
    pub fn new(role: MPCRole, base_port: u16, concurrency: u32) -> Self {
        Self {
            role,
            base_port,
            concurrency,
        }
    }
}
