use crate::error::ActorError;
use crate::link::Exit;
use spawned_rt::tasks::Notify;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// How a bounded mailbox behaves when at capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureMode {
    /// Return [`ActorError::MailboxFull`] immediately.
    #[default]
    FailFast,
    /// Wait until capacity is available.
    Block,
}

/// Mailbox buffering policy for an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxConfig {
    capacity: Option<usize>,
    mode: BackpressureMode,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl MailboxConfig {
    /// No limit on queued user messages (default).
    pub fn unbounded() -> Self {
        Self {
            capacity: None,
            mode: BackpressureMode::FailFast,
        }
    }

    /// Fixed capacity; return [`ActorError::MailboxFull`] when full.
    pub fn bounded(capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be at least 1");
        Self {
            capacity: Some(capacity),
            mode: BackpressureMode::FailFast,
        }
    }

    /// Fixed capacity; block senders until space is available.
    pub fn bounded_blocking(capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be at least 1");
        Self {
            capacity: Some(capacity),
            mode: BackpressureMode::Block,
        }
    }
}

/// Internal mailbox item used uniformly in both `tasks` and `threads` actor loops.
pub(crate) enum MailboxItem<M> {
    Message(M),
    Exit(Exit),
    Shutdown,
}

/// Which runtime an actor mailbox is bound to (affects block-mode waiters).
pub(crate) enum MailboxRuntime {
    Tasks,
    Threads,
}

/// Shared depth counter and block waiters for user message backpressure.
pub(crate) struct MailboxLimits {
    capacity: Option<usize>,
    mode: BackpressureMode,
    depth: AtomicUsize,
    block_cvar: Option<Arc<(Mutex<()>, Condvar)>>,
    block_notify: Option<Arc<Notify>>,
}

impl MailboxLimits {
    pub fn new(config: MailboxConfig, runtime: MailboxRuntime) -> Arc<Self> {
        let needs_block = config.capacity.is_some() && config.mode == BackpressureMode::Block;
        let (block_cvar, block_notify) = match (needs_block, runtime) {
            (true, MailboxRuntime::Threads) => {
                (Some(Arc::new((Mutex::new(()), Condvar::new()))), None)
            }
            (true, MailboxRuntime::Tasks) => (None, Some(Arc::new(Notify::new()))),
            _ => (None, None),
        };
        Arc::new(Self {
            capacity: config.capacity,
            mode: config.mode,
            depth: AtomicUsize::new(0),
            block_cvar,
            block_notify,
        })
    }

    fn try_acquire(&self) -> Result<(), ActorError> {
        let Some(cap) = self.capacity else {
            return Ok(());
        };
        loop {
            let current = self.depth.load(Ordering::Acquire);
            if current >= cap {
                return Err(ActorError::MailboxFull);
            }
            if self
                .depth
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn acquire_blocking_threads(&self) -> Result<(), ActorError> {
        let Some(cap) = self.capacity else {
            return Ok(());
        };
        let block = self
            .block_cvar
            .as_ref()
            .expect("block cvar must exist for threads block mode");
        let (lock, cvar) = &**block;
        let mut guard = lock.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            let current = self.depth.load(Ordering::Acquire);
            if current < cap {
                self.depth.fetch_add(1, Ordering::AcqRel);
                return Ok(());
            }
            guard = cvar.wait(guard).unwrap_or_else(|p| p.into_inner());
        }
    }

    async fn acquire_async_tasks(&self) -> Result<(), ActorError> {
        let Some(cap) = self.capacity else {
            return Ok(());
        };
        let notify = self
            .block_notify
            .as_ref()
            .expect("block notify must exist for tasks block mode");
        loop {
            let current = self.depth.load(Ordering::Acquire);
            if current < cap {
                if self
                    .depth
                    .compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return Ok(());
                }
                continue;
            }
            notify.notified().await;
        }
    }

    fn rollback_acquire(&self) {
        if self.capacity.is_some() {
            self.depth.fetch_sub(1, Ordering::AcqRel);
            self.notify_blockers();
        }
    }

    fn notify_blockers(&self) {
        if self.mode != BackpressureMode::Block {
            return;
        }
        if let Some(block) = &self.block_cvar {
            block.1.notify_all();
        }
        if let Some(notify) = &self.block_notify {
            notify.notify_waiters();
        }
    }

    /// Called when a user message is dequeued (before the handler runs).
    pub fn on_message_dequeued(&self) {
        if self.capacity.is_none() {
            return;
        }
        self.depth.fetch_sub(1, Ordering::AcqRel);
        self.notify_blockers();
    }

    /// Enqueue a user message, applying capacity limits.
    pub fn send_user_threads<M>(
        &self,
        sender: &spawned_rt::threads::mpsc::Sender<M>,
        item: M,
    ) -> Result<(), ActorError> {
        match (self.capacity, self.mode) {
            (None, _) => {}
            (Some(_), BackpressureMode::FailFast) => self.try_acquire()?,
            (Some(_), BackpressureMode::Block) => self.acquire_blocking_threads()?,
        }
        sender.send(item).map_err(|_| {
            self.rollback_acquire();
            ActorError::ActorStopped
        })
    }

    /// Enqueue a user message from a synchronous caller (tasks mode).
    pub fn send_user_tasks_sync<M>(
        &self,
        sender: &spawned_rt::tasks::mpsc::Sender<M>,
        item: M,
    ) -> Result<(), ActorError> {
        match (self.capacity, self.mode) {
            (None, _) => {}
            (Some(_), BackpressureMode::FailFast) => self.try_acquire()?,
            (Some(_), BackpressureMode::Block) => {
                if spawned_rt::tasks::Handle::try_current().is_ok() {
                    spawned_rt::tasks::block_in_place(|| {
                        spawned_rt::tasks::Handle::current().block_on(self.acquire_async_tasks())
                    })?;
                } else {
                    spawned_rt::tasks::block_on(self.acquire_async_tasks())?;
                }
            }
        }
        sender.send(item).map_err(|_| {
            self.rollback_acquire();
            ActorError::ActorStopped
        })
    }

    /// Enqueue a system item (`Exit` or `Shutdown`); never subject to user limits.
    pub fn send_system_threads<M>(
        &self,
        sender: &spawned_rt::threads::mpsc::Sender<M>,
        item: M,
    ) -> Result<(), ActorError> {
        sender.send(item).map_err(|_| ActorError::ActorStopped)
    }

    /// Enqueue a system item (`Exit` or `Shutdown`); never subject to user limits.
    pub fn send_system_tasks<M>(
        &self,
        sender: &spawned_rt::tasks::mpsc::Sender<M>,
        item: M,
    ) -> Result<(), ActorError> {
        sender.send(item).map_err(|_| ActorError::ActorStopped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mailbox_config_constructors() {
        assert!(MailboxConfig::unbounded().capacity.is_none());
        assert_eq!(MailboxConfig::bounded(8).capacity, Some(8));
        assert_eq!(
            MailboxConfig::bounded_blocking(4).mode,
            BackpressureMode::Block
        );
    }

    #[test]
    fn fail_fast_rejects_when_full() {
        let limits = MailboxLimits::new(MailboxConfig::bounded(1), MailboxRuntime::Threads);
        limits.try_acquire().unwrap();
        assert!(matches!(limits.try_acquire(), Err(ActorError::MailboxFull)));
    }
}
