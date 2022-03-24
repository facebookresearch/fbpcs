/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use derive_more::TryInto;

/// The type of data stored in a column
// #[derive(Debug, PartialEq, TryInto)]
#[derive(TryInto, Clone, PartialEq, Debug)]
#[try_into(owned, ref, ref_mut)]
pub enum MPCMetricDType {
    // TODO: Will replace with MPCInt64 and such after FFI is available
    MPCInt32(i32),
    MPCInt64(i64),
    MPCUInt32(u32),
    MPCUInt64(u64),
    MPCBool(bool),
    Vec(Vec<MPCMetricDType>),
}

macro_rules! impl_arithmetic_operator {
    ($trait_name:path, $function_name:ident, $op:tt) => {
        impl $trait_name for MPCMetricDType {
            type Output = Self;

            fn $function_name(self, other: Self) -> Self {
                match (self, other) {
                    (Self::MPCInt32(lhs), Self::MPCInt32(rhs)) => Self::MPCInt32(lhs $op rhs),
                    (Self::MPCInt64(lhs), Self::MPCInt64(rhs)) => Self::MPCInt64(lhs $op rhs),
                    (Self::MPCUInt32(lhs), Self::MPCUInt32(rhs)) => Self::MPCUInt32(lhs $op rhs),
                    (Self::MPCUInt64(lhs), Self::MPCUInt64(rhs)) => Self::MPCUInt64(lhs $op rhs),
                    (Self::Vec(lhs), Self::Vec(rhs)) => Self::Vec(
                        lhs.into_iter()
                            .zip(rhs.into_iter())
                            .map(|(val1, val2)| val1 $op val2)
                            .collect(),
                    ),
                    (scalar_dtype, Self::Vec(vec)) => Self::Vec(
                        vec.into_iter()
                            .map(|vec_element_dtype| scalar_dtype.clone() $op vec_element_dtype)
                            .collect(),
                    ),
                    (Self::Vec(vec), scalar_dtype) => Self::Vec(
                        vec.into_iter()
                            .map(|vec_element_dtype|  vec_element_dtype $op scalar_dtype.clone())
                            .collect(),
                    ),
                    (Self::MPCBool(_lhs), Self::MPCBool(_rhs)) => {
                        panic!("Operator not defined for MPC bool")
                    }
                    (_, _) => panic!("Differing MPCMetricDType variants not supported"),
                }
            }
        }
    };
}

macro_rules! impl_comparision_method {
    ($function_name:ident, $op:tt) => {
            pub fn $function_name(&self, other: &Self) -> Self {
                match (&self, &other) {
                    (Self::MPCInt32(lhs), Self::MPCInt32(rhs)) => Self::MPCBool(lhs $op rhs),
                    (Self::MPCInt64(lhs), Self::MPCInt64(rhs)) => Self::MPCBool(lhs $op rhs),
                    (Self::MPCUInt32(lhs), Self::MPCUInt32(rhs)) => Self::MPCBool(lhs $op rhs),
                    (Self::MPCUInt64(lhs), Self::MPCUInt64(rhs)) => Self::MPCBool(lhs $op rhs),
                    (Self::Vec(lhs), Self::Vec(rhs)) => Self::Vec(
                        lhs.iter()
                            .zip(rhs.iter())
                            .map(|(val1, val2)| val1.$function_name(val2))
                            .collect(),
                    ),
                    (scalar_dtype, Self::Vec(vec)) => Self::Vec(
                        vec.iter()
                            .map(|vec_element_dtype| scalar_dtype.$function_name(vec_element_dtype))
                            .collect(),
                    ),
                    (Self::Vec(vec), scalar_dtype) => Self::Vec(
                        vec.iter()
                            .map(|vec_element_dtype|  vec_element_dtype.$function_name(scalar_dtype))
                            .collect(),
                    ),
                    (Self::MPCBool(_lhs), Self::MPCBool(_rhs)) => {
                        panic!("Operator not defined for MPC bool")
                    }
                    (_, _) => panic!("Differing MPCMetricDType variants not supported"),
                }
            }
    };
}

impl_arithmetic_operator!(std::ops::Add, add, +);
impl_arithmetic_operator!(std::ops::Sub, sub, -);
impl_arithmetic_operator!(std::ops::Mul, mul, *);
impl_arithmetic_operator!(std::ops::Div, div, /);

impl MPCMetricDType {
    pub fn take_inner_val<T>(self) -> Result<T, <T as TryFrom<MPCMetricDType>>::Error>
    where
        T: std::convert::TryFrom<MPCMetricDType>,
        <T as TryFrom<MPCMetricDType>>::Error: std::fmt::Debug,
    {
        T::try_from(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::mpc_metric_dtype::MPCMetricDType;

    #[test]
    fn add() {
        assert_eq!(
            MPCMetricDType::MPCInt32(1) + MPCMetricDType::MPCInt32(2),
            MPCMetricDType::MPCInt32(3)
        );
        assert_eq!(
            MPCMetricDType::MPCInt64(1) + MPCMetricDType::MPCInt64(2),
            MPCMetricDType::MPCInt64(3)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt32(1) + MPCMetricDType::MPCUInt32(2),
            MPCMetricDType::MPCUInt32(3)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt64(1) + MPCMetricDType::MPCUInt64(2),
            MPCMetricDType::MPCUInt64(3)
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(1),
                MPCMetricDType::MPCInt32(1)
            ]) + MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(2),
                MPCMetricDType::MPCInt32(2)
            ]),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(1),
                MPCMetricDType::MPCInt32(1)
            ]) + MPCMetricDType::MPCInt32(2),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
        assert_eq!(
            MPCMetricDType::MPCInt32(2)
                + MPCMetricDType::Vec(vec![
                    MPCMetricDType::MPCInt32(1),
                    MPCMetricDType::MPCInt32(1)
                ]),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
    }

    #[test]
    fn sub() {
        assert_eq!(
            MPCMetricDType::MPCInt32(5) - MPCMetricDType::MPCInt32(2),
            MPCMetricDType::MPCInt32(3)
        );
        assert_eq!(
            MPCMetricDType::MPCInt64(5) - MPCMetricDType::MPCInt64(2),
            MPCMetricDType::MPCInt64(3)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt32(5) - MPCMetricDType::MPCUInt32(2),
            MPCMetricDType::MPCUInt32(3)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt64(5) - MPCMetricDType::MPCUInt64(2),
            MPCMetricDType::MPCUInt64(3)
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(5),
                MPCMetricDType::MPCInt32(5)
            ]) - MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(2),
                MPCMetricDType::MPCInt32(2)
            ]),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(5),
                MPCMetricDType::MPCInt32(5)
            ]) - MPCMetricDType::MPCInt32(2),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
        assert_eq!(
            MPCMetricDType::MPCInt32(4)
                - MPCMetricDType::Vec(vec![
                    MPCMetricDType::MPCInt32(1),
                    MPCMetricDType::MPCInt32(1)
                ]),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
    }

    #[test]
    fn mul() {
        assert_eq!(
            MPCMetricDType::MPCInt32(3) * MPCMetricDType::MPCInt32(2),
            MPCMetricDType::MPCInt32(6)
        );
        assert_eq!(
            MPCMetricDType::MPCInt64(3) * MPCMetricDType::MPCInt64(2),
            MPCMetricDType::MPCInt64(6)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt32(3) * MPCMetricDType::MPCUInt32(2),
            MPCMetricDType::MPCUInt32(6)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt64(3) * MPCMetricDType::MPCUInt64(2),
            MPCMetricDType::MPCUInt64(6)
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ]) * MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(2),
                MPCMetricDType::MPCInt32(2)
            ]),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(6),
                MPCMetricDType::MPCInt32(6)
            ])
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ]) * MPCMetricDType::MPCInt32(2),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(6),
                MPCMetricDType::MPCInt32(6)
            ])
        );
        assert_eq!(
            MPCMetricDType::MPCInt32(3)
                * MPCMetricDType::Vec(vec![
                    MPCMetricDType::MPCInt32(2),
                    MPCMetricDType::MPCInt32(2)
                ]),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(6),
                MPCMetricDType::MPCInt32(6)
            ])
        );
    }

    #[test]
    fn div() {
        assert_eq!(
            MPCMetricDType::MPCInt32(6) / MPCMetricDType::MPCInt32(2),
            MPCMetricDType::MPCInt32(3)
        );
        assert_eq!(
            MPCMetricDType::MPCInt64(6) / MPCMetricDType::MPCInt64(2),
            MPCMetricDType::MPCInt64(3)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt32(6) / MPCMetricDType::MPCUInt32(2),
            MPCMetricDType::MPCUInt32(3)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt64(6) / MPCMetricDType::MPCUInt64(2),
            MPCMetricDType::MPCUInt64(3)
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(6),
                MPCMetricDType::MPCInt32(6)
            ]) / MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(2),
                MPCMetricDType::MPCInt32(2)
            ]),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(6),
                MPCMetricDType::MPCInt32(6)
            ]) / MPCMetricDType::MPCInt32(2),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
        assert_eq!(
            MPCMetricDType::MPCInt32(6)
                / MPCMetricDType::Vec(vec![
                    MPCMetricDType::MPCInt32(2),
                    MPCMetricDType::MPCInt32(2)
                ]),
            MPCMetricDType::Vec(vec![
                MPCMetricDType::MPCInt32(3),
                MPCMetricDType::MPCInt32(3)
            ])
        );
    }

    #[test]
    fn take_inner_val() {
        assert_eq!(
            MPCMetricDType::MPCInt32(32).take_inner_val::<i32>(),
            Ok(32i32)
        );
        assert_eq!(
            MPCMetricDType::MPCInt64(64).take_inner_val::<i64>(),
            Ok(64i64)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt32(32).take_inner_val::<u32>(),
            Ok(32u32)
        );
        assert_eq!(
            MPCMetricDType::MPCUInt64(64).take_inner_val::<u64>(),
            Ok(64u64)
        );
        assert_eq!(
            MPCMetricDType::MPCBool(true).take_inner_val::<bool>(),
            Ok(true)
        );
        assert_eq!(
            MPCMetricDType::Vec(vec![MPCMetricDType::MPCBool(true)])
                .take_inner_val::<Vec::<MPCMetricDType>>(),
            Ok(vec![MPCMetricDType::MPCBool(true)])
        );

        assert!(
            MPCMetricDType::MPCUInt64(6)
                .take_inner_val::<i32>()
                .is_err()
        )
    }

    #[test]
    fn try_into() {
        assert_eq!(MPCMetricDType::MPCInt32(32).try_into(), Ok(32i32));
        assert_eq!(MPCMetricDType::MPCInt64(64).try_into(), Ok(64i64));
        assert_eq!(MPCMetricDType::MPCUInt32(32).try_into(), Ok(32u32));
        assert_eq!(MPCMetricDType::MPCUInt64(64).try_into(), Ok(64u64));
        assert_eq!(MPCMetricDType::MPCBool(true).try_into(), Ok(true));
        assert_eq!(
            MPCMetricDType::Vec(vec![MPCMetricDType::MPCBool(true)]).try_into(),
            Ok(vec![MPCMetricDType::MPCBool(true)])
        );
    }
}
