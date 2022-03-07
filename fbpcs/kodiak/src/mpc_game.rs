/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub struct MPCGame {
    execution_config: ExecutionConfig,
    view: MPCView,
}

impl MPCGame {
    pub fn new(execution_config: ExecutionConfig, view: MPCView) -> Self {
        Self {
            execution_config,
            view,
        }
    }

    pub fn play(&mut self) {
        // TODO
    }

    pub fn reveal(&mut self) {
        // TODO
    }
}
