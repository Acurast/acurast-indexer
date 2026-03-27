use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// Global health state for liveness and readiness checks
pub struct HealthState {
    /// Whether a fatal/unrecoverable error has occurred
    fatal_error: AtomicBool,
    /// Description of the fatal error (if any)
    fatal_error_message: RwLock<Option<String>>,
    /// Whether graceful shutdown is in progress
    shutting_down: AtomicBool,
}

impl HealthState {
    fn new() -> Self {
        Self {
            fatal_error: AtomicBool::new(false),
            fatal_error_message: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Mark the system as having encountered a fatal error
    pub fn set_fatal_error(&self, message: impl Into<String>) {
        self.fatal_error.store(true, Ordering::SeqCst);
        *self.fatal_error_message.write() = Some(message.into());
    }

    /// Check if a fatal error has occurred
    pub fn has_fatal_error(&self) -> bool {
        self.fatal_error.load(Ordering::SeqCst)
    }

    /// Get the fatal error message if any
    #[allow(dead_code)]
    pub fn get_fatal_error_message(&self) -> Option<String> {
        self.fatal_error_message.read().clone()
    }

    /// Mark the system as shutting down
    pub fn set_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    /// Check if the system is shutting down
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }
}

pub static HEALTH_STATE: Lazy<HealthState> = Lazy::new(HealthState::new);
