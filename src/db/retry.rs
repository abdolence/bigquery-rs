use crate::errors::BigQueryError;
use crate::BigQueryDb;
use crate::BigQueryResult;
use gcloud_sdk::tonic::metadata::{MetadataMap, MetadataValue};
use gcloud_sdk::tonic::{Extensions, Request, Response, Status};
use rand::RngExt;
use std::future::Future;
use std::time::Duration;
use tracing::{warn, Span};

impl BigQueryDb {
    /// Sends `message` through `send`, and sends it again after a random [`retry_delay`] while
    /// it fails with a retryable error, up to
    /// [`max_retries`](crate::BigQueryDbOptions::max_retries) times. Each retry is logged in
    /// `span` as "Failed to `action`".
    ///
    /// Only for requests that are safe to send twice: a retried append or DML statement can be
    /// applied twice.
    pub(crate) async fn retry<R, T, F, Fut>(
        &self,
        span: &Span,
        action: &str,
        message: &R,
        send: F,
    ) -> BigQueryResult<T>
    where
        R: Clone,
        F: Fn(Request<R>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.retry_with_metadata(span, action, message, &MetadataMap::new(), send)
            .await
    }

    /// [`retry`](Self::retry), with every attempt carrying `metadata` as request headers, such
    /// as the precondition of [`if_match`].
    pub(crate) async fn retry_with_metadata<R, T, F, Fut>(
        &self,
        span: &Span,
        action: &str,
        message: &R,
        metadata: &MetadataMap,
        send: F,
    ) -> BigQueryResult<T>
    where
        R: Clone,
        F: Fn(Request<R>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        retry(span, action, self.inner.options.max_retries, || {
            send(new_request(message.clone(), metadata))
        })
        .await
        .map(Response::into_inner)
    }
}

/// The `if-match` header that makes a write apply only while the resource still has `etag`,
/// which `read_by` returned. A stale one fails with `FAILED_PRECONDITION`, which
/// [`BigQueryError::on_stale_etag`] reports as a conflict.
///
/// # Errors
/// [`BigQueryError::SystemError`] with the code `UNEXPECTED_RESPONSE` if `etag` is not a valid
/// header value.
pub(crate) fn if_match(read_by: &str, etag: &str) -> BigQueryResult<MetadataMap> {
    let precondition = MetadataValue::try_from(etag).map_err(|_| {
        BigQueryError::unexpected_response(format!(
            "{read_by} returned an etag that is not a valid header: {etag:?}"
        ))
    })?;
    let mut metadata = MetadataMap::new();
    metadata.insert("if-match", precondition);
    Ok(metadata)
}

/// Builds a request for `message` carrying `metadata` as its headers. The channel middleware
/// adds authentication and the client headers on top.
pub(crate) fn new_request<R>(message: R, metadata: &MetadataMap) -> Request<R> {
    Request::from_parts(metadata.clone(), Extensions::default(), message)
}

/// Calls `send` until it succeeds, fails with an error that is not retryable, or has been
/// retried `max_retries` times, waiting [`retry_delay`] between attempts.
pub(crate) async fn retry<T, F, Fut>(
    span: &Span,
    action: &str,
    max_retries: usize,
    send: F,
) -> BigQueryResult<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    let mut retries = 0;
    loop {
        match send().await {
            Ok(response) => return Ok(response),
            Err(status) => {
                let err = BigQueryError::from(status);
                if retries >= max_retries || !err.retry_possible() {
                    return Err(err);
                }
                let delay = retry_delay(retries);
                span.in_scope(|| {
                    warn!(
                        %err,
                        current_retry = retries + 1,
                        max_retries,
                        delay = delay.as_millis(),
                        "Failed to {action}. Retrying up to the specified number of times.",
                    );
                });
                tokio::time::sleep(delay).await;
                retries += 1;
            }
        }
    }
}

/// How long to wait before retry number `retries + 1`: a random delay of up to `2^retries`
/// seconds ("full jitter"), saturating rather than overflowing for large `retries`.
pub(crate) fn retry_delay(retries: usize) -> Duration {
    let max_millis = 2u64
        .saturating_pow(u32::try_from(retries).unwrap_or(u32::MAX))
        .saturating_mul(1000);
    Duration::from_millis(rand::rng().random_range(0..=max_millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_etag_that_is_not_a_header_is_an_unexpected_response() {
        match if_match("GetDataset", "abc\n") {
            Err(BigQueryError::SystemError(err)) => {
                assert_eq!(err.public.code, "UNEXPECTED_RESPONSE", "{err}");
            }
            other => panic!("expected an unexpected response, got {other:?}"),
        }
    }
    use gcloud_sdk::tonic::metadata::MetadataValue;
    use gcloud_sdk::tonic::Code;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn run(
        max_retries: usize,
        failures: usize,
        code: Code,
        attempts: &AtomicUsize,
    ) -> BigQueryResult<&'static str> {
        retry(&Span::none(), "probe", max_retries, || async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < failures {
                Err(Status::new(code, "backend error"))
            } else {
                Ok("done")
            }
        })
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_failures_are_retried_until_success() {
        let attempts = AtomicUsize::new(0);
        let result = run(3, 2, Code::Unavailable, &attempts).await;
        assert_eq!(result.ok(), Some("done"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_stop_after_max_retries() {
        let attempts = AtomicUsize::new(0);
        let result = run(2, usize::MAX, Code::Unavailable, &attempts).await;
        assert!(
            result
                .as_ref()
                .is_err_and(|e| e.has_code(Code::Unavailable)),
            "{result:?}"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn non_retryable_failure_is_sent_once() {
        let attempts = AtomicUsize::new(0);
        let result = run(3, usize::MAX, Code::InvalidArgument, &attempts).await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_max_retries_sends_once() {
        let attempts = AtomicUsize::new(0);
        let result = run(0, usize::MAX, Code::Unavailable, &attempts).await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retry_delay_is_bounded_by_two_to_the_retry_seconds() {
        for retries in 0..6 {
            let bound = Duration::from_secs(1 << retries);
            for _ in 0..100 {
                assert!(retry_delay(retries) <= bound, "retry {retries}");
            }
        }
    }

    #[test]
    fn retry_delay_saturates_for_any_retry_count() {
        for retries in [63, 64, 1000, usize::MAX] {
            assert!(retry_delay(retries) < Duration::MAX);
        }
    }

    #[test]
    fn request_carries_the_given_metadata() {
        let mut metadata = MetadataMap::new();
        metadata.insert("if-match", MetadataValue::from_static("\"etag-1\""));
        let request = new_request("body", &metadata);
        assert_eq!(
            request.metadata().get("if-match").map(|v| v.as_bytes()),
            Some(&b"\"etag-1\""[..])
        );
        assert_eq!(*request.get_ref(), "body");
    }
}
