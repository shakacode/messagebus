use crate::{Bus, Message};
use async_trait::async_trait;

pub trait Handler<M: Message>: Send + Sync {
    type Error: crate::Error;
    type Response: Message;

    fn handle(&self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncHandler<M: Message>: Send + Sync {
    type Error: crate::Error;
    type Response: Message;

    async fn handle(&self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait SynchronizedHandler<M: Message>: Send {
    type Error: crate::Error;
    type Response: Message;

    fn handle(&mut self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncSynchronizedHandler<M: Message>: Send {
    type Error: crate::Error;
    type Response: Message;

    async fn handle(&mut self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait BatchHandler<M: Message>: Send + Sync {
    type Error: crate::Error;
    type Response: Message;

    fn handle(&self, msg: Vec<M>, bus: &Bus) -> Result<Vec<Self::Response>, Self::Error>;
    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncBatchHandler<M: Message>: Send + Sync {
    type Error: crate::Error;
    type Response: Message;

    async fn handle(&self, msg: Vec<M>, bus: &Bus) -> Result<Vec<Self::Response>, Self::Error>;
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait BatchSynchronizedHandler<M: Message>: Send {
    type Error: crate::Error;
    type Response: Message;

    fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Vec<Self::Response>, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncBatchSynchronizedHandler<M: Message>: Send {
    type Error: crate::Error;
    type Response: Message;

    async fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Vec<Self::Response>, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait LocalHandler<M: Message> {
    type Error: crate::Error;
    type Response: Message;

    fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Self::Response, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait LocalAsyncHandler<M: Message> {
    type Error: crate::Error;
    type Response: Message;

    async fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Self::Response, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait LocalBatchHandler<M: Message> {
    type Error: crate::Error;
    type Response: Message;

    fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Vec<Self::Response>, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait LocalAsyncBatchHandler<M: Message> {
    type Error: crate::Error;
    type Response: Message;

    async fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Vec<Self::Response>, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}
