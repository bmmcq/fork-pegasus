use std::fmt::{Debug, Display, Formatter};

use nohash_hasher::IntSet;
use pegasus_common::codec::Buf;
use pegasus_common::tag::Tag;
use pegasus_server::{BufMut, Decode, Encode};

#[derive(Clone, Debug)]
pub enum PeerSet {
    Empty,
    One(u16),
    Range(u16, u16),
    Partial(IntSet<u16>),
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
                if a.len() as u16 == (*r - *l) {
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
            (PeerSet::Range(l, r), PeerSet::One(b)) => *r == *b + 1 && l == b,
            (PeerSet::Range(l, r), PeerSet::Partial(b)) => {
                if b.len() as u16 == (*r - *l) {
                    for i in *l..*r {
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

impl PeerSet {
    pub fn single(index: u16) -> Self {
        PeerSet::One(index)
    }

    pub fn empty() -> Self {
        PeerSet::Empty
    }

    pub fn add_peer(&mut self, index: u16) {
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

    pub fn contains(&self, index: u16) -> bool {
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

    pub fn merge(&mut self, other: PeerSet) {
        match other {
            PeerSet::Empty => {}
            PeerSet::One(v) => self.add_peer(v),
            PeerSet::Range(f, t) => {
                for i in f..t {
                    self.add_peer(i);
                }
            }
            PeerSet::Partial(set) => {
                for i in set {
                    self.add_peer(i)
                }
            }
        }
    }
}

impl Display for PeerSet {
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
    fn write_to<W: BufMut>(&self, _writer: &mut W) {
        todo!()
    }
}

impl Decode for PeerSet {
    fn read_from<R: Buf>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
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
    pub fn new(tag: Tag, parent_peers: PeerSet, total_send: u64, global_total_send: u64) -> Self {
        Eos { tag, total_send, global_total_send, parent_peers, child_peers: PeerSet::empty() }
    }

    pub fn add_child_send(&mut self, to: u16, total_send: usize) {
        if total_send > 0 {
            self.child_peers.add_peer(to);
            self.global_total_send += total_send as u64;
        }
    }

    pub fn merge(&mut self, other: Eos) {
        assert_eq!(self.tag, other.tag);
        self.parent_peers.merge(other.parent_peers);
        self.total_send += other.total_send;
        self.global_total_send += other.global_total_send;
    }

    pub fn has_parent(&self, index: u16) -> bool {
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
    fn write_to<W: BufMut>(&self, _writer: &mut W) {
        todo!()
    }
}

impl Decode for Eos {
    fn read_from<R: Buf>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
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
