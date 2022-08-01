use std::error::Error;

use async_trait::async_trait;
use pegasus_common::bytes::Bytes;
use pegasus_common::config::ServerId;
use pegasus_server::consumer::Consumer;
use pegasus_server::producer::Producer;
use pegasus_server::{Decode, Encode};

use crate::base::intra_process::IntraProcessPush;
use crate::data::Data;
use crate::error::{ErrMsg, IOErrorKind};
use crate::{IOError, Push};

#[async_trait]
pub trait Decoder {
    type Item: Decode;

    async fn decode(&mut self, bytes: Bytes) -> Result<Self::Item, ErrMsg>;
}

pub struct SimpleDecoder<T>(std::marker::PhantomData<T>);

impl<T> SimpleDecoder<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[async_trait]
impl<T> Decoder for SimpleDecoder<T>
where
    T: Decode + Data,
{
    type Item = T;

    async fn decode(&mut self, bytes: Bytes) -> Result<Self::Item, ErrMsg> {
        match T::read_from(&mut bytes.as_ref()) {
            Ok(item) => Ok(item),
            Err(e) => Err(ErrMsg::Own(e.to_string())),
        }
    }
}

pub struct RemotePush<T> {
    target_server: ServerId,
    target_worker: u8,
    send: Producer<T>,
}

impl<T> RemotePush<T> {
    pub fn new(target_server: ServerId, target_worker: u8, send: Producer<T>) -> Self {
        assert_eq!(target_server, send.get_target_server_id());
        Self { target_server, target_worker, send }
    }
}

impl<T: Send + Encode> Push<T> for RemotePush<T> {
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        self.send.send(self.target_worker, msg)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), IOError> {
        self.send.flush()?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.send.close()?;
        Ok(())
    }
}

pub struct RemoteForward<T, D> {
    decoder: D,
    forward: IntraProcessPush<T>,
}

impl<T, D> RemoteForward<T, D> {
    pub fn new(forward: IntraProcessPush<T>, decoder: D) -> Self {
        Self { decoder, forward }
    }
}

#[async_trait]
impl<T, D> Consumer for RemoteForward<T, D>
where
    T: Decode + Data,
    D: Decoder<Item = T> + Send + 'static,
{
    async fn consume(&mut self, msg: Bytes) -> Result<(), Box<dyn Error>> {
        match self.decoder.decode(msg).await {
            Ok(item) => {
                self.forward.push(item)?;
                Ok(())
            }
            Err(msg) => Err(Box::new(IOError::new(IOErrorKind::DecodeError(msg)))),
        }
    }

    async fn close(&mut self) {
        self.forward.close().ok();
    }
}
