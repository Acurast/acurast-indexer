use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TASK_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// What a task is currently processing
#[derive(Debug, Clone, Serialize, Default)]
pub struct CurrentWork {
    /// Block number being processed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<u32>,
    /// Extrinsic ID (block-index format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extrinsic: Option<String>,
    /// Event ID (block-index.event_index format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    /// Phase number for events/extrinsics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<i32>,
    /// Epoch number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
    /// Free-form detail string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Timestamp (millis) when this work was last updated
    pub last_updated: u64,
    /// Previous work item (for debugging state transitions) - only one level deep
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<Box<CurrentWork>>,
}

/// Queue type for metrics tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueueType {
    Event,
    Extrinsic,
    Epoch,
}

/// Metrics for a single queue type
#[derive(Debug, Clone, Serialize, Default)]
pub struct QueueTypeMetrics {
    /// Number of items pending in DB (phase < MAX)
    pub pending_count: i64,
    /// Minimum queued key (formatted string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_queued_key: Option<String>,
    /// Maximum queued key (formatted string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_queued_key: Option<String>,
    /// Items processed per second (30-second rolling average)
    pub throughput_per_sec: f64,
    /// Total items processed since start
    pub items_processed: u64,
}

/// Aggregated queue metrics for all queue types
#[derive(Debug, Clone, Serialize, Default)]
pub struct QueueMetrics {
    pub events: QueueTypeMetrics,
    pub extrinsics: QueueTypeMetrics,
    pub epochs: QueueTypeMetrics,
    /// Timestamp when metrics were last updated
    pub updated_at: u64,
}

/// Tracks throughput using a sliding window of timestamps
#[derive(Debug, Default)]
struct ThroughputTracker {
    /// Window size in milliseconds (30 seconds)
    window_ms: u64,
    /// Timestamps of processed items (in millis)
    timestamps: VecDeque<u64>,
    /// Total items processed
    total: u64,
}

impl ThroughputTracker {
    fn new(window_seconds: u64) -> Self {
        Self {
            window_ms: window_seconds * 1000,
            timestamps: VecDeque::new(),
            total: 0,
        }
    }

    /// Record a processed item
    fn record(&mut self, now: u64) {
        self.timestamps.push_back(now);
        self.total += 1;
        // Remove timestamps older than window
        let cutoff = now.saturating_sub(self.window_ms);
        while self.timestamps.front().map_or(false, |&t| t < cutoff) {
            self.timestamps.pop_front();
        }
    }

    /// Calculate throughput (items per second)
    fn throughput(&self, now: u64) -> f64 {
        let cutoff = now.saturating_sub(self.window_ms);
        let count = self.timestamps.iter().filter(|&&t| t >= cutoff).count();
        let window_secs = self.window_ms as f64 / 1000.0;
        count as f64 / window_secs
    }

    fn total(&self) -> u64 {
        self.total
    }
}

/// Internal state for queue metrics tracking
#[derive(Debug, Default)]
struct QueueMetricsState {
    events: QueueTypeMetricsState,
    extrinsics: QueueTypeMetricsState,
    epochs: QueueTypeMetricsState,
}

#[derive(Debug)]
struct QueueTypeMetricsState {
    pending_count: i64,
    min_queued_key: Option<String>,
    max_queued_key: Option<String>,
    throughput: ThroughputTracker,
}

impl Default for QueueTypeMetricsState {
    fn default() -> Self {
        Self {
            pending_count: 0,
            min_queued_key: None,
            max_queued_key: None,
            throughput: ThroughputTracker::new(30), // 30-second window
        }
    }
}

/// Information about a running task
#[derive(Debug, Clone, Serialize)]
pub struct TaskInfo {
    pub id: u64,
    pub name: String,
    /// Worker index if this is part of a worker pool (e.g., "finalized #0")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_index: Option<u32>,
    pub started_at: u64,
    /// When the task ended (if completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<u64>,
    /// Current work - updated frequently via atomic swap
    pub current_work: CurrentWork,
}

/// Lightweight struct for frequent updates (single field swap)
struct TaskState {
    info: TaskInfo,
    /// Separate field for atomic-like updates of current work
    current_work: RwLock<CurrentWork>,
    /// When the task ended
    ended_at: RwLock<Option<u64>>,
}

pub struct TaskRegistry {
    tasks: RwLock<HashMap<u64, TaskState>>,
    queue_metrics: RwLock<QueueMetricsState>,
}

impl TaskRegistry {
    fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            queue_metrics: RwLock::new(QueueMetricsState::default()),
        }
    }

    /// Start a new task, returns task ID
    pub fn start(&self, name: impl Into<String>, worker_index: Option<u32>) -> u64 {
        let id = TASK_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let now = now_millis();
        let state = TaskState {
            info: TaskInfo {
                id,
                name: name.into(),
                worker_index,
                started_at: now,
                ended_at: None,
                current_work: CurrentWork::default(),
            },
            current_work: RwLock::new(CurrentWork::default()),
            ended_at: RwLock::new(None),
        };

        self.tasks.write().insert(id, state);
        id
    }

    /// Update what a task is currently working on (single write, happens frequently)
    /// Saves the previous work state for debugging (only one level deep)
    pub fn update_work(&self, id: u64, mut work: CurrentWork) {
        if let Some(state) = self.tasks.read().get(&id) {
            let mut current = state.current_work.write();
            // Save current as previous, but strip its previous to keep only one level
            let mut prev = current.clone();
            prev.previous = None;
            work.previous = Some(Box::new(prev));
            work.last_updated = now_millis();
            *current = work;
        }
    }

    /// Convenience: update just the block being processed
    pub fn set_block(&self, id: u64, block: u32) {
        self.update_work(
            id,
            CurrentWork {
                block: Some(block),
                ..Default::default()
            },
        );
    }

    /// Convenience: update just the extrinsic being processed
    pub fn set_extrinsic(&self, id: u64, extrinsic_id: String) {
        self.update_work(
            id,
            CurrentWork {
                extrinsic: Some(extrinsic_id),
                ..Default::default()
            },
        );
    }

    /// Convenience: update just the event being processed
    pub fn set_event(&self, id: u64, event_id: String) {
        self.update_work(
            id,
            CurrentWork {
                event: Some(event_id),
                ..Default::default()
            },
        );
    }

    /// Convenience: update event with phase information
    pub fn set_event_with_phase(&self, id: u64, event_id: String, phase: i32) {
        self.update_work(
            id,
            CurrentWork {
                event: Some(event_id),
                phase: Some(phase),
                ..Default::default()
            },
        );
    }

    /// Convenience: update extrinsic with phase information
    pub fn set_extrinsic_with_phase(&self, id: u64, extrinsic_id: String, phase: i32) {
        self.update_work(
            id,
            CurrentWork {
                extrinsic: Some(extrinsic_id),
                phase: Some(phase),
                ..Default::default()
            },
        );
    }

    /// Convenience: update just the epoch being processed
    pub fn set_epoch(&self, id: u64, epoch: i64) {
        self.update_work(
            id,
            CurrentWork {
                epoch: Some(epoch),
                ..Default::default()
            },
        );
    }

    /// Convenience: set detail text (preserves other fields, updates previous)
    pub fn set_detail(&self, id: u64, detail: String) {
        if let Some(state) = self.tasks.read().get(&id) {
            let mut current = state.current_work.write();
            // Save current as previous (strip its previous to keep only one level)
            let mut prev = current.clone();
            prev.previous = None;
            current.detail = Some(detail);
            current.previous = Some(Box::new(prev));
        }
    }

    /// End a task (keeps it in the registry)
    pub fn end(&self, id: u64) {
        if let Some(state) = self.tasks.read().get(&id) {
            *state.ended_at.write() = Some(now_millis());
        }
    }

    /// Unregister a task (removes it from the registry)
    pub fn delete(&self, id: u64) {
        self.tasks.write().remove(&id);
    }

    // ==================== Queue Metrics Methods ====================

    /// Record that an item was processed for throughput calculation
    pub fn record_processed(&self, queue_type: QueueType) {
        let now = now_millis();
        let mut metrics = self.queue_metrics.write();
        let state = match queue_type {
            QueueType::Event => &mut metrics.events,
            QueueType::Extrinsic => &mut metrics.extrinsics,
            QueueType::Epoch => &mut metrics.epochs,
        };
        state.throughput.record(now);
    }

    /// Update the pending count for a queue type
    pub fn set_pending_count(&self, queue_type: QueueType, count: i64) {
        let mut metrics = self.queue_metrics.write();
        let state = match queue_type {
            QueueType::Event => &mut metrics.events,
            QueueType::Extrinsic => &mut metrics.extrinsics,
            QueueType::Epoch => &mut metrics.epochs,
        };
        state.pending_count = count;
    }

    /// Update the queued key range for a queue type
    pub fn set_queue_range(&self, queue_type: QueueType, min_key: String, max_key: String) {
        let mut metrics = self.queue_metrics.write();
        let state = match queue_type {
            QueueType::Event => &mut metrics.events,
            QueueType::Extrinsic => &mut metrics.extrinsics,
            QueueType::Epoch => &mut metrics.epochs,
        };
        state.min_queued_key = Some(min_key);
        state.max_queued_key = Some(max_key);
    }

    /// Get current queue metrics for all queue types
    pub fn get_queue_metrics(&self) -> QueueMetrics {
        let now = now_millis();
        let metrics = self.queue_metrics.read();

        QueueMetrics {
            events: QueueTypeMetrics {
                pending_count: metrics.events.pending_count,
                min_queued_key: metrics.events.min_queued_key.clone(),
                max_queued_key: metrics.events.max_queued_key.clone(),
                throughput_per_sec: metrics.events.throughput.throughput(now),
                items_processed: metrics.events.throughput.total(),
            },
            extrinsics: QueueTypeMetrics {
                pending_count: metrics.extrinsics.pending_count,
                min_queued_key: metrics.extrinsics.min_queued_key.clone(),
                max_queued_key: metrics.extrinsics.max_queued_key.clone(),
                throughput_per_sec: metrics.extrinsics.throughput.throughput(now),
                items_processed: metrics.extrinsics.throughput.total(),
            },
            epochs: QueueTypeMetrics {
                pending_count: metrics.epochs.pending_count,
                min_queued_key: metrics.epochs.min_queued_key.clone(),
                max_queued_key: metrics.epochs.max_queued_key.clone(),
                throughput_per_sec: metrics.epochs.throughput.throughput(now),
                items_processed: metrics.epochs.throughput.total(),
            },
            updated_at: now,
        }
    }

    /// Get all tasks for the polling endpoint
    pub fn get_all(&self) -> Vec<TaskInfo> {
        self.tasks
            .read()
            .values()
            .map(|state| {
                let mut info = state.info.clone();
                // Merge in the current work and ended_at
                info.current_work = state.current_work.read().clone();
                info.ended_at = *state.ended_at.read();
                info
            })
            .collect()
    }
}

pub static TASK_REGISTRY: Lazy<TaskRegistry> = Lazy::new(TaskRegistry::new);

/// A guard that ensures a task is properly ended when dropped.
/// This handles both normal completion and panics.
pub struct TaskGuard {
    task_id: u64,
    /// Error message to report when dropped (if any)
    error: Option<String>,
    /// Whether the task completed successfully (suppresses "dropped without complete" warning)
    completed: bool,
}

impl TaskGuard {
    /// Create a new task guard by starting a task in the registry.
    pub fn new(name: impl Into<String>, worker_index: Option<u32>) -> Self {
        let task_id = TASK_REGISTRY.start(name, worker_index);
        Self {
            task_id,
            error: None,
            completed: false,
        }
    }

    /// Get the task ID for this guard.
    pub fn id(&self) -> u64 {
        self.task_id
    }

    /// Set an error message that will be reported when the guard is dropped.
    pub fn set_error(&mut self, error: impl ToString) {
        self.error = Some(error.to_string());
        // Also immediately set the detail so it's visible before drop
        TASK_REGISTRY.set_detail(
            self.task_id,
            format!("ERROR: {}", self.error.as_ref().unwrap()),
        );
    }

    /// Record an error from a Result, returning the original Result unchanged.
    /// Useful for chaining: `result.inspect_err(|e| guard.record_error(e))?`
    pub fn record_error<E: ToString>(&mut self, error: &E) {
        self.set_error(error.to_string());
    }

    /// Mark the task as completed successfully.
    /// This prevents the "task dropped without completion" warning.
    pub fn complete(mut self) {
        self.completed = true;
        // Drop will be called, which will call end()
    }

    /// Mark the task as completed with an error.
    pub fn complete_with_error(mut self, error: impl ToString) {
        self.set_error(error);
        self.completed = true;
        // Drop will be called, which will call end() and set the error detail
    }

    // Delegate common methods to TASK_REGISTRY for convenience
    pub fn set_block(&self, block: u32) {
        TASK_REGISTRY.set_block(self.task_id, block);
    }

    pub fn set_extrinsic(&self, extrinsic_id: String, phase: i32) {
        TASK_REGISTRY.set_extrinsic_with_phase(self.task_id, extrinsic_id, phase);
    }

    pub fn set_event(&self, event_id: String, phase: i32) {
        TASK_REGISTRY.set_event_with_phase(self.task_id, event_id, phase);
    }

    pub fn set_epoch(&self, epoch: i64) {
        TASK_REGISTRY.set_epoch(self.task_id, epoch);
    }

    pub fn set_detail(&self, detail: String) {
        TASK_REGISTRY.set_detail(self.task_id, detail);
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        // Check if we're panicking
        if std::thread::panicking() && self.error.is_none() {
            self.error = Some("task panicked".to_string());
        }

        // Set the error detail if there was an error
        if let Some(ref error) = self.error {
            TASK_REGISTRY.set_detail(self.task_id, format!("ERROR: {}", error));
        } else if !self.completed {
            // Task was dropped without being marked complete (likely early return or ?)
            TASK_REGISTRY.set_detail(self.task_id, "task ended unexpectedly".to_string());
        }

        // Always end the task
        TASK_REGISTRY.end(self.task_id);
    }
}
