/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub struct MPCGame {
    mpc_config: MPCConfig,
    metrics: MPCMetrics,
}

impl MPCGame {
    pub fn new(mpc_config: MPCConfig, metrics: MPCMetrics) -> Self {
        Self {
            mpc_config,
            metrics,
        }
    }

    pub fn play(&mut self) {
        // TODO
    }

    pub fn reveal(&mut self) {
        // TODO
    }
}
