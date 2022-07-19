use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::{Add, AddAssign, Div};

use nohash_hasher::IntSet;
use pegasus_common::codec::Encode;

use crate::codec::{Decode, ReadExt, WriteExt};
use crate::progress::PeerSet::Partial;
use crate::Tag;

#[derive(Clone, Debug)]
enum PeerSet {
    Empty,
    One(u32),
    Range(u32, u32),
    Partial(IntSet<u32>),
}

impl PartialEq for PeerSet {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PeerSet::Empty, PeerSet::Empty) => true,
            (PeerSet::Empty, _) => false,
            (_, PeerSet::Empty) => false,
            (PeerSet::One(a), PeerSet::One(b)) => a == b,
            (PeerSet::One(a), PeerSet::Partial(b)) => b.len() == 1 && b.contains(a),
            (PeerSet::One(a), PeerSet::Range(l, r)) => *a == *l && *r == *a + 1,
            (PeerSet::Partial(a), PeerSet::One(b)) => a.len() == 1 && a.contains(b),
            (PeerSet::Partial(a), PeerSet::Partial(b)) => a == b,
            (PeerSet::Partial(a), PeerSet::Range(l, r)) => {
                if a.len() as u32 == (*r - *l) {
                    for i in *l..*r {
                        if !a.contains(&i) {
                            return false;
                        }
                    }
                    true
                } else {
                    false
                }
            }
            (PeerSet::Range(l, r), PeerSet::One(b)) => *r == *b + 1 && *l == b,
            (PeerSet::Range(l, r), PeerSet::Partial(b)) => {
                if b.len() as u32 == (*r - *l) {
                    for i in 0..*a {
                        if !b.contains(&i) {
                            return false;
                        }
                    }
                    true
                } else {
                    false
                }
            }
            (PeerSet::Range(l1, r1), PeerSet::Range(l2, r2)) => l1 == l2 && r1 == r2,
        }
    }
}

impl Eq for PeerSet {}

#[derive(Clone, PartialEq, Eq)]
pub struct DynPeers {
    mask: PeerSet,
}

impl PeerSet {
    pub fn single(index: u32) -> Self {
        PeerSet::One(index)
    }

    pub fn empty() -> Self {
        PeerSet::Empty
    }

    pub fn add_peer(&mut self, index: u32) {
        match self {
            PeerSet::Empty => *self = PeerSet::One(index),
            PeerSet::One(a) => {
                let mut set = IntSet::default();
                set.insert(*a);
                set.insert(index);
                *self = PeerSet::Partial(set)
            }
            PeerSet::Partial(set) => {
                set.insert(index);
            }
            PeerSet::Range(l, r) => {
                if index == *l - 1 {
                    *l -= 1;
                } else if index == *r {
                    *r += 1;
                } else if index < *l || index > *r {
                    let mut set = IntSet::default();
                    for i in *l..*r {
                        set.insert(i);
                    }
                    set.insert(index);
                    *self = PeerSet::Partial(set);
                }
            }
        }
    }

    pub fn contains(&self, index: u32) -> bool {
        match self {
            PeerSet::Empty => false,
            PeerSet::One(id) => *id == index,
            PeerSet::Partial(set) => set.contains(&index),
            PeerSet::Range(l, r) => index >= *l && index < *r,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            PeerSet::Empty => 0,
            PeerSet::One(_) => 1,
            PeerSet::Partial(ref set) => set.len(),
            PeerSet::Range(l, r) => (*r - *l) as usize,
        }
    }

    pub fn merge(&mut self, mut other: PeerSet) {
        match (self, &mut other) {
            (PeerSet::Empty, _) => {
                *self = other;
            }
            (_, PeerSet::Empty) => (),
            (PeerSet::One(a), PeerSet::One(b)) => {
                let mut set = IntSet::default();
                set.insert(*a);
                set.insert(*b);
                *self = PeerSet::Partial(set);
            }
            (PeerSet::One(a), PeerSet::Partial(b)) => {
                b.insert(*a);
                *self = other;
            }
            (PeerSet::One(a), PeerSet::Range(l, r)) => {
                if *a >= *l && *a < *r {
                    *self = other;
                } else if *a == *l - 1 {
                    *l -= 1;
                    *self = other;
                } else if *a == *r {
                    *r += 1;
                    *self = other;
                } else {
                    let mut set = IntSet::default();
                    for i in *l..*r {
                        set.insert(i);
                    }
                    set.insert(*a);
                    *self = Partial(set);
                }
            }
            (PeerSet::Partial(a), PeerSet::One(b)) => {
                a.insert(*b);
            }
            (PeerSet::Partial(a), PeerSet::Partial(b)) => {
                a.extend(b);
            }
            (PeerSet::Partial(a), PeerSet::Range(l, r)) => {
                for i in *l..*r {
                    a.insert(i);
                }
                if a.len() as u32 == (*r - *l) {
                    *self = other;
                }
            }
            (PeerSet::Range(l, r), PeerSet::One(b)) => {
                if *b == *l - 1 {
                    *l -= 1;
                } else if *b == *r {
                    *r += 1;
                } else if *b < *l || *b > *r {
                    let mut set = IntSet::default();
                    for i in *l..*r {
                        set.insert(i);
                    }
                    set.insert(*b);
                    *self = Partial(set);
                }
            }
            (PeerSet::Range(l, r), PeerSet::Partial(set)) => {
                for i in *l..*r {
                    set.insert(i);
                }
                if set.len() as u32 > (*r - *l) {
                    *self = other;
                }
            }
            (PeerSet::Range(l1, r1), PeerSet::Range(l2, r2)) => {
                *l1 = std::cmp::min(*l1, *l2);
                *r1 = std::cmp::max(*r1, *r2);
            }
        }
    }
}

impl Debug for PeerSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerSet::Empty => write!(f, "P[]"),
            PeerSet::One(x) => write!(f, "P[{}]", x),
            PeerSet::Partial(ref p) => write!(f, "P{:?}", p),
            PeerSet::Range(l, r) => write!(f, "P[{}..{}]", l, r),
        }
    }
}

impl Encode for PeerSet {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            PeerSet::Empty => writer.write_u8(0),
            PeerSet::One(x) => {
                writer.write_u8(1)?;
                writer.write_u32(*x)
            }
            PeerSet::Partial(s) => {
                writer.write_u8(2)?;
                writer.write_u32(s.len() as u32)?;
                for x in s.iter() {
                    writer.write_u32(*x)?;
                }
                Ok(())
            }
            PeerSet::Range(l, r) => {
                writer.write_u8(3)?;
                writer.write_u32(*l)?;
                writer.write_u32(*r)
            }
        }
    }
}

impl Decode for PeerSet {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let mode = reader.read_u8()?;
        if mode == 0 {
            Ok(PeerSet::Empty)
        } else if mode == 1 {
            let x = reader.read_u32()?;
            Ok(mask: PeerSet::One(x))
        } else if mode == 2 {
            let len = reader.read_u32()? as usize;
            let mut set = IntSet::default();
            for _ in 0..len {
                let x = reader.read_u32()?;
                set.insert(x);
            }
            Ok(PeerSet::Partial(set))
        } else if mode == 3 {
            let from = reader.read_u32()?;
            let to = reader.read_u32()?;
            Ok(PeerSet::Range(from, to))
        } else {
            Err(std::io::ErrorKind::InvalidData)?
        }
    }
}

#[derive(Clone)]
pub struct Eos {
    /// The tag of scope this end belongs to;
    pub(crate) tag: Tag,
    /// Record how many data has send to target consumer;
    pub(crate) total_send: u64,
    /// Record how many data has send to all consumers;
    pub(crate) global_total_send: u64,
    /// The worker peers who also has send data(and end) of this scope;
    /// It indicates how many the `[EndOfScope]` will be received by consumers;
    parent_peers: PeerSet,
    child_peers: PeerSet,
}

impl Eos {
    pub(crate) fn new(tag: Tag, parent_peers: PeerSet, total_send: u64, global_total_send: u64) -> Self {
        Eos { tag, total_send, global_total_send, parent_peers, child_peers: PeerSet::empty() }
    }

    pub(crate) fn add_child_send(&mut self, to: u32, total_send: usize) {
        if total_send > 0 {
            self.child_peers.add_peer(to);
            self.global_total_send += total_send as u64;
        }
    }

    pub(crate) fn merge(&mut self, other: Eos) {
        assert_eq!(self.tag, other.tag);
        self.parent_peers.merge(other.parent_peers);
        self.total_send += other.total_send;
        self.global_total_send += other.global_total_send;
    }

    pub fn has_parent(&self, index: u32) -> bool {
        self.parent_peers.contains(index)
    }

    pub fn parent_peers(&self) -> &PeerSet {
        &self.parent_peers
    }

    pub fn child_peers(&self) -> &PeerSet {
        &self.child_peers
    }
}

impl Debug for Eos {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "end({:?}_{})", self.tag, self.total_send)
    }
}

impl Encode for Eos {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(self.total_send)?;
        writer.write_u64(self.global_total_send)?;
        self.tag.write_to(writer)?;
        self.parent_peers.write_to(writer)?;
        self.child_peers.write_to(writer)
    }
}

impl Decode for Eos {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let total_send = reader.read_u64()?;
        let global_total_send = reader.read_u64()?;
        let tag = Tag::read_from(reader)?;
        let parent_peers = PeerSet::read_from(reader)?;
        let child_peers = PeerSet::read_from(reader)?;
        Ok(Eos { tag, total_send, global_total_send, parent_peers, child_peers })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn peer_set_eq_test() {
        assert_eq!(PeerSet::One(0), PeerSet::One(0));
        assert_ne!(PeerSet::One(0), PeerSet::One(1));

        let mut set = IntSet::default();
        set.insert(1);
        assert_eq!(PeerSet::One(1), PeerSet::Partial(set));

        let mut set = IntSet::default();
        set.insert(1);
        set.insert(2);
        assert_ne!(PeerSet::One(1), PeerSet::Partial(set));

        assert_eq!(PeerSet::One(0), PeerSet::Range(0, 1));
        assert_ne!(PeerSet::One(1), PeerSet::Range(0, 1));

        let mut set = IntSet::default();
        set.insert(1);
        assert_eq!(PeerSet::Partial(set), PeerSet::One(1));

        let mut set1 = IntSet::default();
        set1.insert(0);
        set1.insert(1);
        let mut set2 = IntSet::default();
        set2.insert(0);
        set2.insert(1);
        assert_eq!(PeerSet::Partial(set1), PeerSet::Partial(set2));

        let mut set = IntSet::default();
        set.insert(0);
        set.insert(1);
        set.insert(2);
        assert_eq!(PeerSet::Partial(set), PeerSet::Range(0, 3));

        let mut set1 = IntSet::default();
        set1.insert(0);
        set1.insert(1);
        let mut set2 = IntSet::default();
        set2.insert(0);
        set2.insert(1);
        set2.insert(2);
        assert_ne!(PeerSet::Partial(set1), PeerSet::Partial(set2));

        assert_eq!(PeerSet::Range(0, 1), PeerSet::One(0));
        assert_eq!(PeerSet::Range(0, 3), PeerSet::Range(0, 3));
        let mut set = IntSet::default();
        set.insert(0);
        set.insert(1);
        set.insert(2);
        assert_eq!(PeerSet::Range(0, 3), PeerSet::Partial(set));
    }
}
