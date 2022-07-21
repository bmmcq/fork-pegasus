use std::fmt::Debug;

use pegasus_common::codec::{Codec, Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use pegasus_common::tag::Tag;

use crate::buffer::batch::{RoBatch, WoBatch};
use crate::eos::Eos;

/// The constraint of data that can be delivered through the channel;
pub trait Data: Send + Codec + 'static {}

impl<T: Send + Codec + 'static> Data for T {}

pub struct Item<T> {
    pub tag: Tag,
    data: Option<T>,
    eos: Option<Eos>,
}

impl<T> Item<T> {
    pub fn new(tag: Tag, data: Option<T>, eos: Option<Eos>) -> Self {
        Self { tag, data, eos }
    }

    pub fn data(tag: Tag, item: T) -> Self {
        Self { tag, data: Some(item), eos: None }
    }

    pub fn get_data(&self) -> Option<&T> {
        self.data.as_ref()
    }

    pub fn take_data(&mut self) -> Option<T> {
        self.data.take()
    }

    pub fn get_eos(&self) -> Option<&Eos> {
        self.eos.as_ref()
    }

    pub fn take_eos(&mut self) -> Option<Eos> {
        self.eos.take()
    }
}

pub struct MiniBatch<T> {
    /// the index of worker who created this batch;
    pub src: u16,
    data: RoBatch<Item<T>>,
}

impl<T> MiniBatch<T> {
    pub fn drain(&mut self) -> impl Iterator<Item = Item<T>> + '_ {
        self.data.drain()
    }
}

impl<T: Encode> Encode for MiniBatch<T> {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        todo!()
    }
}

impl<T: Decode> Decode for MiniBatch<T> {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
    }
}

pub struct MiniScopeBatch<T> {
    /// the tag of the scope of this bath ;
    pub tag: Tag,
    /// the index of worker who created this batch;
    pub src: u16,
    end: Option<Eos>,
    data: RoBatch<T>,
}

impl<D> MiniScopeBatch<D> {
    pub fn empty() -> Self {
        MiniScopeBatch { tag: Tag::Null, src: 0, end: None, data: RoBatch::default() }
    }

    pub fn new(tag: Tag, src: u16, data: RoBatch<D>) -> Self {
        MiniScopeBatch { tag, src, end: None, data }
    }

    pub fn set_end(&mut self, end: Eos) {
        self.end = Some(end);
    }

    pub fn set_tag(&mut self, tag: Tag) {
        if let Some(end) = self.end.as_mut() {
            end.tag = tag.clone();
        }
        self.tag = tag;
    }

    pub fn get_end_mut(&mut self) -> Option<&mut Eos> {
        self.end.as_mut()
    }

    pub fn take_end(&mut self) -> Option<Eos> {
        self.end.take()
    }

    pub fn take_data(&mut self) -> RoBatch<D> {
        std::mem::replace(&mut self.data, RoBatch::default())
    }

    pub fn clear(&mut self) {
        self.take_data();
    }

    pub fn is_last(&self) -> bool {
        self.end.is_some()
    }

    pub fn get_end(&self) -> Option<&Eos> {
        self.end.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn tag(&self) -> &Tag {
        &self.tag
    }
}

impl<D> MiniScopeBatch<D> {
    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = D> + '_ {
        self.data.drain()
    }
}

impl<D> Debug for MiniScopeBatch<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "batch[{:?} len={}]", self.tag, self.data.len())
    }
}

impl<D> std::ops::Deref for MiniScopeBatch<D> {
    type Target = RoBatch<D>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<D> std::ops::DerefMut for MiniScopeBatch<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<D: Clone> Clone for MiniScopeBatch<D> {
    fn clone(&self) -> Self {
        MiniScopeBatch {
            tag: self.tag.clone(),
            src: self.src,
            end: self.end.clone(),
            data: self.data.clone(),
        }
    }
}

impl<D: Encode> Encode for MiniScopeBatch<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.tag.write_to(writer)?;
        writer.write_u16(self.src)?;
        let len = self.data.len() as u64;
        writer.write_u64(len)?;

        for data in self.data.iter() {
            data.write_to(writer)?;
        }
        self.end.write_to(writer)?;
        Ok(())
    }
}

impl<D: Decode> Decode for MiniScopeBatch<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let tag = Tag::read_from(reader)?;
        let src = reader.read_u16()?;
        let len = reader.read_u64()? as usize;
        let mut buf = WoBatch::new(len);
        for _ in 0..len {
            buf.push(D::read_from(reader)?);
        }
        let data = buf.finalize();
        let end = Option::<Eos>::read_from(reader)?;
        Ok(MiniScopeBatch { tag, src, end, data })
    }
}