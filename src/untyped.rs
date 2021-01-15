use core::any::Any;
use core::future::Future;
use core::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug)]
pub enum Lock<'a, T> {
    ReadOnly(&'a T),
    RwRead(RwLockReadGuard<'a, T>),
    RwWrite(RwLockWriteGuard<'a, T>),
    WriteOnly(MutexGuard<'a, T>),
}

impl<'a, T> Lock<'a, T> {
    pub fn get_ref(&self) -> &T {
        match self {
            Lock::ReadOnly(inner) => &inner,
            Lock::RwRead(inner) => &inner,
            Lock::RwWrite(inner) => &inner,
            Lock::WriteOnly(inner) => &inner,
        }
    }

    pub fn get_mut(&mut self) -> &mut T {
        match self {
            Lock::ReadOnly(_) => panic!("!!"),
            Lock::RwRead(_) => panic!("!!"),
            Lock::RwWrite(inner) => &mut *inner,
            Lock::WriteOnly(inner) => &mut *inner,
        }
    }
}

pub enum Downcasted<T> {
    ReadOnly(Arc<T>),
    ReadWrite(Arc<RwLock<T>>),
    WriteOnly(Arc<Mutex<T>>),
}

impl<T> Clone for Downcasted<T> {
    fn clone(&self) -> Self {
        match self {
            Downcasted::ReadOnly(inner) => Downcasted::ReadOnly(inner.clone()),
            Downcasted::ReadWrite(inner) => Downcasted::ReadWrite(inner.clone()),
            Downcasted::WriteOnly(inner) => Downcasted::WriteOnly(inner.clone()),
        }
    }
}

impl<T: 'static> Downcasted<T> {
    pub async fn lock_read(&self) -> Lock<'_, T> {
        match self {
            Downcasted::ReadOnly(inner) => Lock::ReadOnly(&inner),
            Downcasted::ReadWrite(inner) => Lock::RwRead(inner.read().await),
            Downcasted::WriteOnly(inner) => Lock::WriteOnly(inner.lock().await),
        }
    }
    pub async fn lock_write(&self) -> Lock<'_, T> {
        match self {
            Downcasted::ReadOnly(_) => unimplemented!(),
            Downcasted::ReadWrite(inner) => Lock::RwWrite(inner.write().await),
            Downcasted::WriteOnly(inner) => Lock::WriteOnly(inner.lock().await),
        }
    }
}

#[derive(Clone)]
pub struct Untyped {
    inner: Arc<dyn Any + Send + Sync>,
}

impl Untyped {
    pub fn new_readonly<T: Send + Sync + 'static>(item: T) -> Self {
        Self {
            inner: Arc::new(item),
        }
    }

    pub fn new_rwlock<T: Send + Sync + 'static>(item: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(item)),
        }
    }

    pub fn new_mutex<T: Send + 'static>(item: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(item)),
        }
    }

    pub fn new_local<T: 'static, F: FnOnce() -> T + Send + 'static>(f: F) -> Self {
        Self {
            inner: Arc::new(ThreadDedicated::new(f)),
        }
    }

    pub fn downcast_sync<T: Send + Sync + 'static>(self) -> Option<Downcasted<T>> {
        let item = match self.inner.clone().downcast::<RwLock<T>>() {
            Ok(inner) => Downcasted::ReadWrite(inner),
            Err(_) => return None,
        };

        Some(item)
    }

    pub fn downcast_send1<T: Send + 'static>(self) -> Option<Downcasted<T>> {
        let item = match self.inner.clone().downcast::<Mutex<T>>() {
            Ok(inner) => Downcasted::WriteOnly(inner),
            Err(_) => return None,
        };

        Some(item)
    }

    pub fn downcast_send<T: Send + 'static>(self) -> Option<Arc<Mutex<T>>> {
        self.inner.clone().downcast::<Mutex<T>>().ok()
    }

    #[inline]
    pub fn downcast_local<T: 'static>(self) -> Option<Arc<ThreadDedicated<T>>> {
        self.inner.clone().downcast::<ThreadDedicated<T>>().ok()
    }
}

pub struct ThreadDedicated<T: 'static> {
    sender: mpsc::Sender<
        Box<dyn for<'a> FnOnce(&'a mut T) -> Pin<Box<dyn Future<Output = ()> + 'a>> + Send>,
    >,
    notify: Arc<Notify>,
}

impl<T: 'static> ThreadDedicated<T> {
    pub fn new<F: FnOnce() -> T + Send + 'static>(builder: F) -> Self {
        let notify = Arc::new(Notify::new());
        let (sender, mut receiver) = mpsc::channel(1);

        let sender: mpsc::Sender<
            Box<dyn for<'a> FnOnce(&'a mut T) -> Pin<Box<dyn Future<Output = ()> + 'a>> + Send>,
        > = sender;
        let notify_clone = notify.clone();
        std::thread::spawn(move || {
            futures::executor::block_on(async move {
                let mut obj = builder();

                loop {
                    let cb = match receiver.recv().await {
                        Some(x) => x,
                        None => break,
                    };

                    cb(&mut obj).await;
                    notify_clone.notify_one();
                }
            });
        });

        Self { sender, notify }
    }

    pub async fn spawn_local<
        F: for<'a> FnOnce(&'a mut T) -> Pin<Box<dyn Future<Output = ()> + 'a>> + Send + 'static,
    >(
        &self,
        cb: F,
    ) {
        self.sender.send(Box::new(cb)).await.ok().unwrap();

        self.notify.notified().await
    }
}
