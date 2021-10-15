use core::iter::FromIterator;
use std::ops::ControlFlow;

use crate::{error::StdSyncSendError, Bus, Message};
use async_trait::async_trait;

#[async_trait]
pub trait AsyncProducer<'a, M: Message>: Send {
    type Error: StdSyncSendError;
    type Context: Send + 'a;
    type Response: Message;

    async fn start(&'a self, msg: M, bus: &Bus) -> Result<Self::Context, Self::Error>;
    async fn next(&'a self, ctx: &mut Self::Context, bus: &Bus) -> Result<ControlFlow<Self::Response>, Self::Error>;
    async fn finish(&'a self, _ctx: Self::Context, _bus: &Bus) -> Result<(), Self::Error>;
}


pub trait Handler<M: Message>: Send + Sync {
    type Error: StdSyncSendError;
    type Response: Message;

    fn handle(&self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncHandler<M: Message>: Send + Sync {
    type Error: StdSyncSendError;
    type Response: Message;

    async fn handle(&self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait SynchronizedHandler<M: Message>: Send {
    type Error: StdSyncSendError;
    type Response: Message;

    fn handle(&mut self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncSynchronizedHandler<M: Message>: Send {
    type Error: StdSyncSendError;
    type Response: Message;

    async fn handle(&mut self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait BatchHandler<M: Message>: Send + Sync {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    fn handle(&self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;
    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncBatchHandler<M: Message>: Send + Sync {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    async fn handle(&self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait BatchSynchronizedHandler<M: Message>: Send {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    fn handle(&mut self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncBatchSynchronizedHandler<M: Message>: Send {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    async fn handle(
        &mut self,
        msg: Self::InBatch,
        bus: &Bus,
    ) -> Result<Self::OutBatch, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait LocalHandler<M: Message> {
    type Error: StdSyncSendError;
    type Response: Message;

    fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Self::Response, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait LocalAsyncHandler<M: Message> {
    type Error: StdSyncSendError;
    type Response: Message;

    async fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Self::Response, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait LocalBatchHandler<M: Message> {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    fn handle(&mut self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait LocalAsyncBatchHandler<M: Message> {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    async fn handle(
        &mut self,
        msg: Self::InBatch,
        bus: &Bus,
    ) -> Result<Self::OutBatch, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}
