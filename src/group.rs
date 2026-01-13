//! Task grouping support for the message bus.
//!
//! This module provides functionality for grouping related tasks together,
//! enabling operations like flushing all tasks belonging to a specific group.
//!
//! # Usage
//!
//! Messages can define a group ID using the `#[group_id]` derive attribute:
//!
//! ```rust,no_run
//! use messagebus::derive::Message;
//!
//! #[derive(Debug, Clone, Message)]
//! #[group_id(self.job_id)]
//! struct ProcessChunk {
//!     job_id: i64,
//!     chunk_index: usize,
//!     data: Vec<u8>,
//! }
//! ```
//!
//! Then use `Bus::flush_group` to wait for all tasks in a group:
//!
//! ```rust,no_run
//! # async fn example(bus: &messagebus::Bus) {
//! // Wait for all tasks with group_id = 42 to complete
//! bus.flush_group(42).await;
//! # }
//! ```

use dashmap::{DashMap, DashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::Notify;

/// Type alias for group identifiers.
///
/// Uses `i64` for database compatibility. Can be changed in the future
/// if a different type is needed.
pub type GroupId = i64;

/// Internal entry for tracking a single group's state.
struct GroupEntry {
    /// Count of in-flight tasks for this group.
    processing: AtomicU64,
    /// Notification channel for when the group becomes idle.
    /// Wrapped in Arc so we can clone it and await after dropping the DashMap reference.
    idle_notify: Arc<Notify>,
    /// Receiver IDs that have processed messages for this group.
    /// Used for targeted sync operations.
    receivers: DashSet<u64>,
}

impl GroupEntry {
    fn new() -> Self {
        Self {
            processing: AtomicU64::new(0),
            idle_notify: Arc::new(Notify::new()),
            receivers: DashSet::new(),
        }
    }
}

/// Registry for tracking task groups.
///
/// This structure maintains per-group counters for tracking in-flight tasks
/// and provides methods for waiting until a group becomes idle.
pub struct GroupRegistry {
    groups: DashMap<GroupId, GroupEntry>,
}

impl Default for GroupRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupRegistry {
    /// Creates a new empty group registry.
    pub fn new() -> Self {
        Self {
            groups: DashMap::new(),
        }
    }

    /// Increments the processing count for a group and tracks the receiver.
    ///
    /// Called when a new task is spawned for a message with this group ID.
    /// The receiver_id is tracked so that `sync_group` can sync only the
    /// relevant receivers.
    pub fn increment(&self, group_id: GroupId, receiver_id: u64) {
        let entry = self.groups.entry(group_id).or_insert_with(GroupEntry::new);
        entry.processing.fetch_add(1, Ordering::SeqCst);
        entry.receivers.insert(receiver_id);
    }

    /// Decrements the processing count for a group.
    ///
    /// Called when a task completes. If this was the last task in the group,
    /// waiters are notified.
    pub fn decrement(&self, group_id: GroupId) {
        if let Some(entry) = self.groups.get(&group_id) {
            let prev = entry.processing.fetch_sub(1, Ordering::SeqCst);
            if prev == 1 {
                // This was the last task, notify waiters
                entry.idle_notify.notify_waiters();
            }
        }
    }

    /// Returns `true` if the group has no in-flight tasks.
    ///
    /// Returns `true` for unknown groups (they have no tasks by definition).
    pub fn is_idle(&self, group_id: GroupId) -> bool {
        self.groups
            .get(&group_id)
            .map(|e| e.processing.load(Ordering::SeqCst) == 0)
            .unwrap_or(true)
    }

    /// Returns the current processing count for a group.
    ///
    /// Returns 0 for unknown groups.
    pub fn processing_count(&self, group_id: GroupId) -> u64 {
        self.groups
            .get(&group_id)
            .map(|e| e.processing.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Waits until the group becomes idle (has no in-flight tasks).
    ///
    /// Returns immediately if the group is already idle or doesn't exist.
    pub async fn wait_idle(&self, group_id: GroupId) {
        loop {
            let Some(entry) = self.groups.get(&group_id) else {
                // Group doesn't exist, so it's idle by definition
                return;
            };

            if entry.processing.load(Ordering::SeqCst) == 0 {
                return;
            }

            // Clone the Arc<Notify> so we can await after dropping the DashMap reference
            let notify = Arc::clone(&entry.idle_notify);
            // Explicitly drop the entry to release the DashMap lock
            drop(entry);

            // Now we can safely await without holding the DashMap reference
            notify.notified().await;
        }
    }

    /// Removes a group from the registry.
    ///
    /// Use this to clean up groups that are no longer needed to prevent
    /// memory growth in long-running applications.
    ///
    /// Returns `true` if the group was removed, `false` if it didn't exist.
    pub fn remove(&self, group_id: GroupId) -> bool {
        self.groups.remove(&group_id).is_some()
    }

    /// Returns the number of tracked groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Returns the receiver IDs that have handled messages for a group.
    ///
    /// Returns an empty Vec for unknown groups.
    pub fn receivers_for_group(&self, group_id: GroupId) -> Vec<u64> {
        self.groups
            .get(&group_id)
            .map(|entry| entry.receivers.iter().map(|r| *r).collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_decrement() {
        let registry = GroupRegistry::new();

        assert!(registry.is_idle(1));
        assert_eq!(registry.processing_count(1), 0);

        registry.increment(1, 100); // group 1, receiver 100
        assert!(!registry.is_idle(1));
        assert_eq!(registry.processing_count(1), 1);

        registry.increment(1, 100); // same receiver
        assert_eq!(registry.processing_count(1), 2);

        registry.decrement(1);
        assert_eq!(registry.processing_count(1), 1);

        registry.decrement(1);
        assert!(registry.is_idle(1));
        assert_eq!(registry.processing_count(1), 0);
    }

    #[test]
    fn test_multiple_groups() {
        let registry = GroupRegistry::new();

        registry.increment(1, 100);
        registry.increment(2, 200);
        registry.increment(2, 201);

        assert_eq!(registry.processing_count(1), 1);
        assert_eq!(registry.processing_count(2), 2);
        assert!(registry.is_idle(3));

        registry.decrement(1);
        assert!(registry.is_idle(1));
        assert!(!registry.is_idle(2));
    }

    #[test]
    fn test_remove() {
        let registry = GroupRegistry::new();

        registry.increment(1, 100);
        assert_eq!(registry.group_count(), 1);

        assert!(registry.remove(1));
        assert_eq!(registry.group_count(), 0);

        assert!(!registry.remove(1)); // Already removed
    }

    #[tokio::test]
    async fn test_wait_idle() {
        let registry = std::sync::Arc::new(GroupRegistry::new());

        // Waiting on non-existent group should return immediately
        registry.wait_idle(999).await;

        // Waiting on idle group should return immediately
        registry.increment(1, 100);
        registry.decrement(1);
        registry.wait_idle(1).await;

        // Test waiting for completion
        let registry_clone = registry.clone();
        registry.increment(1, 100);

        let handle = tokio::spawn(async move {
            registry_clone.wait_idle(1).await;
        });

        // Give the spawned task time to start waiting
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Decrement should unblock the waiter
        registry.decrement(1);

        // The spawned task should complete
        tokio::time::timeout(tokio::time::Duration::from_millis(100), handle)
            .await
            .expect("timeout waiting for task")
            .expect("task panicked");
    }

    #[test]
    fn test_receivers_for_group() {
        let registry = GroupRegistry::new();

        // Unknown group returns empty
        assert!(registry.receivers_for_group(999).is_empty());

        // Track receivers for a group
        registry.increment(1, 100);
        registry.increment(1, 200);
        registry.increment(1, 100); // duplicate receiver, should not add again

        let receivers = registry.receivers_for_group(1);
        assert_eq!(receivers.len(), 2);
        assert!(receivers.contains(&100));
        assert!(receivers.contains(&200));

        // Different group has different receivers
        registry.increment(2, 300);
        let receivers2 = registry.receivers_for_group(2);
        assert_eq!(receivers2.len(), 1);
        assert!(receivers2.contains(&300));
    }
}
