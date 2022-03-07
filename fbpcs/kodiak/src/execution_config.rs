/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

struct ExecutionConfig {
    network_config: NetworkConfig,
    operation_config: OperationConfig,
}

impl ExecutionConfig {
    fn new(network_config: NetworkConfig, operation_config: OperationConfig) -> Self {
        Self {
            network_config,
            operation_config,
        }
    }
}
