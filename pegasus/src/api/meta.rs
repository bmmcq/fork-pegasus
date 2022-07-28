//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.


#[derive(Clone)]
pub struct OperatorInfo {
    pub name: String,
    pub index: usize,
    pub scope_level: u32,
}

impl std::fmt::Debug for OperatorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}_{}]", self.name, self.index)
    }
}

impl OperatorInfo {
    pub fn new(name: &str, index: usize, scope_level: u32) -> Self {
        OperatorInfo { name: name.to_owned(), index, scope_level }
    }
}
