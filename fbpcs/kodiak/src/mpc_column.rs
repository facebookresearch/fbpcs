/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub trait MPCColumn {
    /// The type of data stored in this column
    type DType;

    /// Used to get keys for our map
    fn name() -> String;

    /// List of columns required by this column
    fn requires() -> Vec<Box<dyn MPCColumn>>;

    /// Compute this value - assume requirements are satisfied
    fn compute(&mut self, r: Row) -> Result<()>;

    /// Aggregate this row with another (basically enable `reduce`)
    fn aggregate(&self, other: &Self) -> Self;

    /// Get the *value* for this metric as JSON
    /// This is a hack to work around the type system.
    /// One metric can return "123" while another can
    /// return "[200, 100, 20, 3, 0]" - the compiler will
    /// allow both since they're just strings.
    fn json_value(&self) -> String;

    /// Retrieve the data in this column
    fn data(&self) -> DType;
}
