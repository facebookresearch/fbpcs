/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;
use crate::execution_config::ExecutionConfig;
use crate::mpc_view::MPCView;

pub struct MPCGame<T: ColumnMetadata> {
    execution_config: ExecutionConfig,
    view: MPCView<T>,
}

impl<T: ColumnMetadata> MPCGame<T> {
    pub fn new(execution_config: ExecutionConfig, view: MPCView<T>) -> Self {
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
