//! A `tracing` layer that keeps the events, the log lines, one thread emits, so a test can
//! assert what a log line carries.
//!
//! It is installed in the same global subscriber as [`spans`](super::spans), and a test opts in
//! per thread for the same reason. An event belongs to the capture active on the thread that
//! emitted it, so the code under test must be polled on the test's own thread, as
//! `#[tokio::test]`'s current-thread runtime does.

use super::spans::{Fields, Visitor};
use std::cell::RefCell;
use std::sync::{Arc, Mutex, PoisonError};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

/// Every event emitted on one thread since [`capture`](Self::capture), in order, with its level
/// and its fields; the text of the message is the field `message`.
#[derive(Clone, Default)]
pub(crate) struct CapturedEvents(Arc<Mutex<Vec<(Level, Fields)>>>);

thread_local! {
    static ACTIVE: RefCell<Option<CapturedEvents>> = const { RefCell::new(None) };
}

/// Ends a thread's capture when dropped.
pub(crate) struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.borrow_mut().take());
    }
}

impl CapturedEvents {
    /// Captures the events this thread emits until the guard drops.
    pub fn capture() -> (Self, CaptureGuard) {
        super::spans::install_global_subscriber();
        let events = Self::default();
        ACTIVE.with(|active| *active.borrow_mut() = Some(events.clone()));
        (events, CaptureGuard)
    }

    /// The fields of every event at `level`.
    pub fn at(&self, level: Level) -> Vec<Fields> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(l, _)| *l == level)
            .map(|(_, fields)| fields.clone())
            .collect()
    }
}

/// The global layer: it hands each event to the capture active on the emitting thread.
pub(super) struct EventRouter;

impl<S: Subscriber> Layer<S> for EventRouter {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let Some(capture) = ACTIVE.with(|active| active.borrow().clone()) else {
            return;
        };
        let mut fields = Fields::new();
        event.record(&mut Visitor(&mut fields));
        capture
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*event.metadata().level(), fields));
    }
}
