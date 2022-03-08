/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::operation_config::OperationConfig;

pub struct OperationConfigBuilder {
    concurrency: u32,
    log_level: log::Level,
    debug: bool,
}

impl OperationConfigBuilder {
    pub fn new() -> Self {
        Self {
            concurrency: 1,
            log_level: log::Level::Info,
            debug: false,
        }
    }

    pub fn with_concurrency(&mut self, concurrency: u32) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_log_level(&mut self, log_level: log::Level) -> Self {
        self.log_level = log_level;
        self
    }

    pub fn with_debug(&mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }

    pub fn build(&self) -> OperationConfig {
        OperationConfig::new(self.concurrency, self.log_level, self.debug)
    }
}
