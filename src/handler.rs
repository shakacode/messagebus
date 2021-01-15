use crate::{Bus, Message};
use async_trait::async_trait;

pub trait Handler<M: Message>: Send + Sync {
    fn handle(&self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncHandler<M: Message>: Send + Sync {
    async fn handle(&self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait SynchronizedHandler<M: Message>: Send {
    fn handle(&mut self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncSynchronizedHandler<M: Message>: Send {
    async fn handle(&mut self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait BatchHandler<M: Message>: Send + Sync {
    fn handle(&self, msg: Vec<M>, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncBatchHandler<M: Message>: Send + Sync {
    async fn handle(&self, msg: Vec<M>, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait BatchSynchronizedHandler<M: Message>: Send {
    fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncBatchSynchronizedHandler<M: Message>: Send {
    async fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait LocalHandler<M: Message> {
    fn handle(&mut self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait(?Send)]
pub trait LocalAsyncHandler<M: Message> {
    async fn handle(&mut self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

pub trait LocalBatchHandler<M: Message> {
    fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait(?Send)]
pub trait LocalAsyncBatchHandler<M: Message> {
    async fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {
        Ok(())
    }
}
