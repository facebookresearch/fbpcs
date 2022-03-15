/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

pub trait InputReader {
    fn next_line(&mut self) -> String;
    fn read_all(&self) -> Vec<String>;
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
    fn next_line(&mut self) -> String {
        unimplemented!();
    }

    fn read_all(&self) -> Vec<String> {
        let file = File::open(&self.filepath).expect("no such file");
        let buf = BufReader::new(file);
        buf.lines()
            .map(|l| l.expect("Could not parse line"))
            .collect()
    }
}
