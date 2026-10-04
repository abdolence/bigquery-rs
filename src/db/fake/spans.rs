//! A `tracing` layer that keeps the fields every span was created and recorded with, so a test
//! can assert what a span carries.
//!
//! The layer is installed once as the global subscriber rather than as each test's scoped
//! default: `tracing` caches whether a callsite is enabled, and a test thread running with no
//! subscriber can cache "never" for a span another thread's scoped subscriber wanted. Each
//! test opts in per thread instead, and a span belongs to the capture that was active on the
//! thread that created it.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, Once, PoisonError};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// The fields of one span: names to values, strings unquoted and everything else in its
/// `Debug` form.
pub(crate) type Fields = BTreeMap<String, String>;

/// Every span created on one thread since [`capture`](Self::capture), in creation order.
#[derive(Clone, Default)]
pub(crate) struct CapturedSpans(Arc<Mutex<Vec<(String, Fields)>>>);

/// Where a span's fields are kept, in the span's extensions.
struct Slot(CapturedSpans, usize);

/// The global layer: it hands each new span to the capture active on the creating thread.
struct Router;

thread_local! {
    static ACTIVE: RefCell<Option<CapturedSpans>> = const { RefCell::new(None) };
}

/// Ends a thread's capture when dropped.
pub(crate) struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.borrow_mut().take());
    }
}

impl CapturedSpans {
    /// Captures the spans this thread creates until the guard drops. Fields recorded later on
    /// such a span, from any task or thread, still arrive.
    pub fn capture() -> (Self, CaptureGuard) {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            let _ = tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(Router),
            );
        });
        let spans = Self::default();
        ACTIVE.with(|active| *active.borrow_mut() = Some(spans.clone()));
        (spans, CaptureGuard)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(String, Fields)>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The fields of every span named `name`.
    pub fn named(&self, name: &str) -> Vec<Fields> {
        self.lock()
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, fields)| fields.clone())
            .collect()
    }

    /// The fields of the one span named `name`.
    ///
    /// # Panics
    /// Unless exactly one span has that name.
    pub fn only(&self, name: &str) -> Fields {
        let mut spans = self.named(name);
        assert_eq!(spans.len(), 1, "one {name:?} span, got {spans:?}");
        spans.remove(0)
    }

    /// Waits until the one span named `name` has `field`, for a field recorded by a task that
    /// ends after the call under test returned, and returns its fields.
    pub async fn wait_for(&self, name: &str, field: &str) -> Fields {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(fields) = self
                    .named(name)
                    .into_iter()
                    .find(|fields| fields.contains_key(field))
                {
                    return fields;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{name:?} recorded {field:?}: {:?}", self.named(name)))
    }
}

struct Visitor<'a>(&'a mut Fields);

impl Visit for Visitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for Router
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(capture) = ACTIVE.with(|active| active.borrow().clone()) else {
            return;
        };
        let mut fields = Fields::new();
        attrs.record(&mut Visitor(&mut fields));
        let index = {
            let mut spans = capture.lock();
            spans.push((attrs.metadata().name().to_string(), fields));
            spans.len() - 1
        };
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(Slot(capture, index));
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let extensions = span.extensions();
        let Some(Slot(capture, index)) = extensions.get::<Slot>() else {
            return;
        };
        let mut spans = capture.lock();
        if let Some((_, fields)) = spans.get_mut(*index) {
            values.record(&mut Visitor(fields));
        }
    }
}

/// The fields `"/bigquery/<name>" = value` of `pairs`, as a span records them.
pub(crate) fn bigquery_fields(pairs: &[(&str, &str)]) -> Fields {
    pairs
        .iter()
        .map(|(name, value)| (format!("/bigquery/{name}"), value.to_string()))
        .collect()
}
