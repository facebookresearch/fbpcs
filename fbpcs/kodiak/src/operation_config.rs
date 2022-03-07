/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub struct OperationConfig {
    concurrency: u32,
    log_level: log::Level,
    debug: bool,
}

impl OperationConfig {
    pub fn new(concurrency: u32, log_level: log::Level, debug: bool) -> Self {
        Self {
            concurrency,
            log_level,
            debug,
        }
    }
}
