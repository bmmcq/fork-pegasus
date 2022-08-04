use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::BufMut;
use pegasus_common::config::ServerId;
use valley::codec::Encode;
use valley::errors::SendError;
use valley::send::unbound::VUnboundServerSender;

pub struct Producer<T> {
    cp_cnt: Option<Arc<AtomicUsize>>,
    send: VUnboundServerSender<Package<T>>,
}

pub struct Package<T> {
    flag: u8,
    data: Option<T>,
}

impl<T> Package<T> {
    fn data_of(index: u8, data: T) -> Self {
        Self { flag: index, data: Some(data) }
    }

    fn eof() -> Self {
        Self { flag: 0, data: None }
    }
}

impl<T: Encode> Encode for Package<T> {
    fn write_to<W: BufMut>(&self, writer: &mut W) {
        writer.put_u8(self.flag);
        if let Some(d) = self.data.as_ref() {
            d.write_to(writer);
        }
    }
}

impl<T> Producer<T> {
    pub fn new(send: VUnboundServerSender<Package<T>>) -> Self {
        Self { cp_cnt: Some(Arc::new(AtomicUsize::new(1))), send }
    }

    pub fn get_target_server_id(&self) -> ServerId {
        self.send.get_target_server_id() as ServerId
    }

    pub fn send(&self, consumer_index: u8, data: T) -> Result<(), SendError> {
        self.send
            .send(Package::data_of(consumer_index, data))
    }

    pub fn flush(&self) -> Result<(), SendError> {
        self.send.flush()
    }

    pub fn close(&mut self) -> Result<(), SendError> {
        if let Some(cnt) = self.cp_cnt.take() {
            if cnt.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.send.send(Package::eof())?;
            }
        }
        Ok(())
    }
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        if let Some(cp_cnt) = self.cp_cnt.as_ref() {
            cp_cnt.fetch_add(1, Ordering::SeqCst);
        }
        let cp_cnt = self.cp_cnt.clone();
        Self { cp_cnt, send: self.send.clone() }
    }
}
