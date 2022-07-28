use pegasus_common::config::ServerId;
use pegasus_server::Encode;
use pegasus_server::producer::Producer;
use crate::{IOError, Push};

pub struct RemotePush<T> {
    target_server: ServerId,
    target_worker: u8,
    send: Producer<T>
}

impl <T> RemotePush<T> {
    pub fn new(target_server: ServerId, target_worker: u8, send: Producer<T>) -> Self {
        assert_eq!(target_server, send.get_target_server_id());
        Self {
            target_server,
            target_worker,
            send
        }
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