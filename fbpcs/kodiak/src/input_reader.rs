/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs::File;
use std::io::{BufRead, BufReader};

pub trait InputReader {
    fn read(&self) -> Box<dyn Iterator<Item = String>>;
}

// TODO(T114390321): [BE][Kodiak] move LocalInputReader to its own file
pub struct LocalInputReader {
    filepath: String,
}

impl LocalInputReader {
    pub fn new(filepath: &str) -> Self {
        Self {
            filepath: filepath.to_string(),
        }
    }
}

impl InputReader for LocalInputReader {
    fn read(&self) -> Box<dyn Iterator<Item = String>> {
        let file = File::open(&self.filepath).expect("no such file");
        let buf = BufReader::new(file);
        Box::new(buf.lines().map(|line| line.expect("Could not read line")))
    }
}
