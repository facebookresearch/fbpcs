/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub struct Row {
    // Dynamic columns because we don't really care about the underlying "data"
    // This allows us to track which columns have already been calculated
    // This struct would likely handle building the DAG to know which columns can
    // be computed next.
    columns: HashMap<String, Box<dyn MPCColumn>>,
}

impl Row {
    fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    fn get_data<T: MPCColumn>(&self) -> Option<T::DType> {
        self.columns
            .get(T.name())?
            .as_any()
            .downcast_ref::<T>()
            .data()
    }
}
