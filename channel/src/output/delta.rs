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

use pegasus_common::tag::Tag;

/// Describing how data's scope will be changed when being send from an output port.
#[derive(Debug, Copy, Clone)]
pub enum ScopeDelta {
    /// The scope won't be changed.
    None,
    /// Go to a next adjacent sibling scope;
    ToSibling,
    /// Go to child scope;
    ToChild,
    /// Go to parent scope;
    ToParent,
}

impl Default for ScopeDelta {
    fn default() -> Self {
        ScopeDelta::None
    }
}

impl ScopeDelta {
    pub fn evolve(&self, tag: &Tag) -> Tag {
        match self {
            ScopeDelta::None => tag.clone(),
            ScopeDelta::ToSibling => {
               tag.advance()
            }
            ScopeDelta::ToChild => {
                Tag::inherit(tag, 0)
            }
            ScopeDelta::ToParent => {
                tag.to_parent_uncheck()
            }
        }
    }
}