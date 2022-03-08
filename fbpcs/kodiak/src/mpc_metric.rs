/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;
use crate::row::Row;

/// The type of data stored in a column
pub enum MPCMetricDType {
    // TODO: Will replace with MPCInt64 and such after FFI is available
    MPCInt32(i32),
    MPCInt64(i64),
    MPCUInt32(u32),
    MPCUInt64(u64),
    MPCBool(bool),
    Vec(Vec<MPCMetricDType>),
}

pub trait MPCMetric {
    /// Used to look up name and dependencies for this metric
    /// ColumnMetadata should be a singleton known at compile time,
    /// so &'static is fine
    fn column_metadata(&self) -> &'static dyn ColumnMetadata;

    /// Compute this value - assume requirements are satisfied
    fn compute(&mut self, r: Row) -> Result<(), ()>;

    /// Aggregate this row with another (basically enable `reduce`)
    fn aggregate(&self, other: &dyn MPCMetric) -> dyn MPCMetric;

    /// Get the *value* for this metric as JSON
    /// This is a hack to work around the type system.
    /// One metric can return "123" while another can
    /// return "[200, 100, 20, 3, 0]" - the compiler will
    /// allow both since they're just strings.
    fn json_value(&self) -> String;

    /// Retrieve the data in this column
    // TODO: Should be able to "force" the type to the actual inner type
    // Probably use some sort of template here to get discriminator and panic
    // otherwise... maybe just functions like as_int32() in the enum?
    fn data(&self) -> MPCMetricDType;
}
