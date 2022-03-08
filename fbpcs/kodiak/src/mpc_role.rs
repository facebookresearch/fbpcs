/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str::FromStr;

#[derive(Debug)]
pub struct ParseMPCRoleError {
    s: String,
}

impl ParseMPCRoleError {
    fn new(s: &str) -> ParseMPCRoleError {
        ParseMPCRoleError { s: s.to_string() }
    }
}

pub enum MPCRole {
    Publisher,
    Partner,
}

impl FromStr for MPCRole {
    type Err = ParseMPCRoleError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "publisher" => Ok(MPCRole::Publisher),
            "partner" => Ok(MPCRole::Partner),
            _ => Err(Self::Err::new(s)),
        }
    }
}
