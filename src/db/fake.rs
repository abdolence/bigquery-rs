//! A fake BigQuery gRPC server for driving a real [`BigQueryDb`] against a controlled backend.
//!
//! It speaks raw HTTP/2, so a handler sees each request message as it arrives and answers in
//! any shape gRPC allows: a unary reply, a server stream, a bidi exchange one request at a
//! time, a status at any point, a dropped connection or a call that never answers.
//!
//! [`FakeServer`], [`FakeCall`] and the [`wire`] encodings are shared with `bigquery::testing`.
//! [`FakeBigQuery`] and the per-API helpers in the sibling `read`, `write`, `query` and `table`
//! modules serve the crate's own tests.

#[cfg(test)]
pub(crate) mod events;
#[cfg(test)]
pub(crate) mod query;
#[cfg(test)]
pub(crate) mod read;
#[cfg(test)]
pub(crate) mod spans;
#[cfg(test)]
pub(crate) mod table;
pub(crate) mod wire;
#[cfg(test)]
pub(crate) mod write;

use crate::db::RetryBackoff;
#[cfg(test)]
use crate::{BigQueryDatasetId, BigQueryTableId};
use crate::{BigQueryDb, BigQueryDbOptions, BigQueryResult};
use futures::future::BoxFuture;
use gcloud_sdk::prost::{DecodeError, Message};
use gcloud_sdk::tonic::Code;
use h2::server::SendResponse;
use h2::{RecvStream, SendStream};
use hyper::body::Bytes;
use hyper::header::HeaderValue;
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
#[cfg(test)]
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinSet};

/// The `shop` dataset the fake-server tests use, in the client's own project.
#[cfg(test)]
pub(crate) const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
/// The `orders` table the fake-server tests use, in [`SHOP`].
#[cfg(test)]
pub(crate) const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");

type Handler = dyn Fn(FakeCall) -> BoxFuture<'static, ()> + Send + Sync;

/// Every line the handlers of one server logged, in order.
#[cfg(test)]
type CallLog = Arc<watch::Sender<Vec<String>>>;

/// A gRPC server on a loopback port that hands every call to one handler.
///
/// The handler runs once per RPC, on its own task, and owns the call until it answers. Dropping
/// the server stops it: the accept task owns the connections, and each connection owns its
/// calls, so aborting the accept task ends every task the server started.
pub(crate) struct FakeServer {
    endpoint: url::Url,
    accept: AbortHandle,
    #[cfg(test)]
    calls: CallLog,
}

/// What every call on one server shares.
#[derive(Clone)]
struct CallContext {
    handler: Arc<Handler>,
    #[cfg(test)]
    calls: CallLog,
}

impl FakeServer {
    /// Binds a free loopback port and starts serving it with `handler`.
    pub(crate) async fn bind<F, Fut>(handler: F) -> std::io::Result<Self>
    where
        F: Fn(FakeCall) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = url::Url::parse(&format!("http://{}", listener.local_addr()?))
            .map_err(std::io::Error::other)?;
        let context = CallContext {
            handler: Arc::new(move |call| Box::pin(handler(call))),
            #[cfg(test)]
            calls: Arc::new(watch::Sender::new(Vec::new())),
        };
        #[cfg(test)]
        let calls = context.calls.clone();
        let accept = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((socket, _)) = listener.accept().await {
                while connections.try_join_next().is_some() {}
                connections.spawn(serve_connection(socket, context.clone()));
            }
            // The listener failed: keep serving the connections already open.
            while connections.join_next().await.is_some() {}
        })
        .abort_handle();
        Ok(Self {
            endpoint,
            accept,
            #[cfg(test)]
            calls,
        })
    }

    /// A client whose v2 and Storage endpoints both point at this server, with the project,
    /// location and `max_retries` of `options`. It authenticates with a token the server never
    /// checks.
    pub(crate) async fn client(
        &self,
        options: BigQueryDbOptions,
        backoff: RetryBackoff,
    ) -> BigQueryResult<BigQueryDb> {
        let options = options
            .with_bigquery_api_url(self.endpoint.clone())
            .with_bigquery_storage_api_url(self.endpoint.clone());
        BigQueryDb::connect(
            options,
            Vec::new(),
            gcloud_sdk::TokenSourceType::ExternalSource(Box::new(FakeTokenSource)),
            backoff,
        )
        .await
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// A running fake server and a [`BigQueryDb`] whose v2 and Storage endpoints both point at it.
///
/// The handler records what it saw with [`FakeCall::log`], so a test can assert a whole RPC
/// sequence with one comparison of [`FakeBigQuery::calls`].
#[cfg(test)]
pub(crate) struct FakeBigQuery {
    pub db: BigQueryDb,
    server: FakeServer,
}

#[cfg(test)]
impl FakeBigQuery {
    pub async fn start<F, Fut>(handler: F) -> Self
    where
        F: Fn(FakeCall) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self::start_with_max_retries(3, handler).await
    }

    /// Like [`start`](Self::start), with a `max_retries` other than the client default, so a
    /// test can bound a persistent failure to a few fast attempts.
    pub async fn start_with_max_retries<F, Fut>(max_retries: usize, handler: F) -> Self
    where
        F: Fn(FakeCall) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let server = FakeServer::bind(handler)
            .await
            .expect("a free loopback port for the fake server");
        let options = BigQueryDbOptions::new("fake-project".into()).with_max_retries(max_retries);
        let db = server
            .client(options, RetryBackoff::FullJitter)
            .await
            .expect("a client for the fake server");
        Self { db, server }
    }

    /// Every line the handlers logged so far, in the order they logged them.
    pub fn calls(&self) -> Vec<String> {
        self.server.calls.borrow().clone()
    }

    /// Resolves once at least `count` lines have been logged.
    pub async fn wait_for_calls(&self, count: usize) {
        self.server
            .calls
            .subscribe()
            .wait_for(|calls| calls.len() >= count)
            .await
            .expect("the call log lives as long as the server that owns it");
    }
}

/// One RPC as the server sees it: the request messages as they arrive, and the response.
pub(crate) struct FakeCall {
    method: String,
    headers: hyper::HeaderMap,
    body: RecvStream,
    received: Vec<u8>,
    respond: SendResponse<Bytes>,
    send: Option<SendStream<Bytes>>,
    close: Arc<Notify>,
    #[cfg(test)]
    calls: CallLog,
}

impl FakeCall {
    /// The RPC's method name, such as `ReadRows`, without its service.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The request header `name` as text, if the call carries it.
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    }

    /// Appends one line to the server's call log.
    #[cfg(test)]
    pub fn log(&self, line: impl Into<String>) {
        let line = line.into();
        self.calls.send_modify(|calls| calls.push(line));
    }

    /// The next request message's bytes, or `None` once the client has closed its side.
    pub async fn next_message(&mut self) -> Option<Vec<u8>> {
        loop {
            if self.received.len() >= 5 {
                let mut length = [0u8; 4];
                length.copy_from_slice(&self.received[1..5]);
                let length = u32::from_be_bytes(length) as usize;
                if self.received.len() >= 5 + length {
                    let message = self.received[5..5 + length].to_vec();
                    self.received.drain(..5 + length);
                    return Some(message);
                }
            }
            let chunk = self.body.data().await?.ok()?;
            let _ = self.body.flow_control().release_capacity(chunk.len());
            self.received.extend_from_slice(&chunk);
        }
    }

    /// The next request message, decoded, or `None` once the client has closed its side.
    ///
    /// # Errors
    /// The decode error if the bytes are not an `M`.
    pub async fn try_next_request<M: Message + Default>(
        &mut self,
    ) -> Result<Option<M>, DecodeError> {
        match self.next_message().await {
            Some(bytes) => M::decode(bytes.as_slice()).map(Some),
            None => Ok(None),
        }
    }

    /// The next request message, decoded.
    ///
    /// # Panics
    /// If the bytes are not an `M`, which is a test bug.
    #[cfg(test)]
    pub async fn next_request<M: Message + Default>(&mut self) -> Option<M> {
        self.try_next_request()
            .await
            .expect("the request decodes as the handler's type")
    }

    /// Sends the response headers now. [`send`](Self::send) does it on the first message; a
    /// bidi handler that waits for requests before answering calls this first, so the client's
    /// call resolves.
    pub fn open(&mut self) {
        if self.send.is_some() {
            return;
        }
        let headers = ok_headers()
            .body(())
            .expect("static response headers are valid");
        self.send = self.respond.send_response(headers, false).ok();
    }

    /// Sends one response message.
    pub fn send<M: Message>(&mut self, message: &M) {
        self.open();
        let message = message.encode_to_vec();
        let length = u32::try_from(message.len()).expect("a test response is smaller than 4 GiB");
        let mut frame = Vec::with_capacity(5 + message.len());
        frame.push(0);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&message);
        if let Some(send) = &mut self.send {
            let _ = send.send_data(frame.into(), false);
        }
    }

    /// Ends the call with status OK.
    pub fn finish(self) {
        self.end(Code::Ok, "");
    }

    /// Answers a unary call: one message, then status OK.
    pub fn reply<M: Message>(mut self, message: &M) {
        self.send(message);
        self.finish();
    }

    /// Ends the call with `code` and `message`. Before any response message it is a
    /// Trailers-Only response, the only form in which a streaming call's error shows at its
    /// initial `await` rather than once the stream is polled.
    pub fn fail(self, code: Code, message: &str) {
        assert_ne!(code, Code::Ok, "end a call successfully with finish()");
        self.end(code, message);
    }

    fn end(mut self, code: Code, message: &str) {
        let status = (code as i32).to_string();
        let message = percent_encode(message);
        match &mut self.send {
            Some(send) => {
                let mut trailers = hyper::HeaderMap::new();
                trailers.insert(
                    "grpc-status",
                    HeaderValue::from_str(&status).expect("a status code is a valid header"),
                );
                if !message.is_empty() {
                    trailers.insert(
                        "grpc-message",
                        HeaderValue::from_str(&message)
                            .expect("percent-encoded text is a valid header"),
                    );
                }
                let _ = send.send_trailers(trailers);
            }
            None => {
                let mut headers = ok_headers().header("grpc-status", status);
                if !message.is_empty() {
                    headers = headers.header("grpc-message", message);
                }
                let headers = headers.body(()).expect("the status headers are valid");
                let _ = self.respond.send_response(headers, true);
            }
        }
    }

    /// Closes the whole connection without answering, as a response lost in transit: the
    /// client sees a transport error, not a status. Never returns, since dropping the call
    /// while the connection is up would reset only this stream, which reads as `Cancelled`.
    pub async fn drop_connection(self) {
        self.close.notify_one();
        std::future::pending::<()>().await;
    }

    /// Never answers and keeps the call open, as a call stuck in flight.
    pub async fn hang(self) {
        std::future::pending::<()>().await;
    }
}

/// Accepts any token, since the fake server checks none.
struct FakeTokenSource;

#[async_trait::async_trait]
impl gcloud_sdk::Source for FakeTokenSource {
    async fn token(&self) -> gcloud_sdk::error::Result<gcloud_sdk::Token> {
        Ok(gcloud_sdk::Token::new(
            "Bearer".to_string(),
            "fake-token".to_string().into(),
            jiff::Timestamp::MAX,
        ))
    }
}

/// Serves one client connection until the client closes it or a handler drops it. The calls
/// it started end with it, since a call cannot answer once its connection is gone.
async fn serve_connection(socket: TcpStream, context: CallContext) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    let close = Arc::new(Notify::new());
    let mut calls = JoinSet::new();
    let serve = async {
        while let Some(Ok((request, respond))) = connection.accept().await {
            let method = request
                .uri()
                .path()
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_string();
            let headers = request.headers().clone();
            let call = FakeCall {
                method,
                headers,
                body: request.into_body(),
                received: Vec::new(),
                respond,
                send: None,
                close: close.clone(),
                #[cfg(test)]
                calls: context.calls.clone(),
            };
            while calls.try_join_next().is_some() {}
            calls.spawn((context.handler)(call));
        }
    };
    tokio::select! {
        () = serve => {}
        () = close.notified() => {}
    }
}

fn ok_headers() -> hyper::http::response::Builder {
    hyper::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
}

/// `grpc-message` is percent-encoded outside printable ASCII, and `%` itself.
fn percent_encode(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    for byte in message.bytes() {
        if (0x20..=0x7e).contains(&byte) && byte != b'%' {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::BigQueryError;
    use futures::StreamExt;
    use gcloud_sdk::google::cloud::bigquery::storage::v1::{
        AppendRowsRequest, AppendRowsResponse, ReadRowsRequest, ReadRowsResponse,
    };
    use gcloud_sdk::google::cloud::bigquery::v2::{GetTableRequest, Table};

    #[tokio::test]
    async fn unary_call_is_answered_and_logged() {
        let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
            assert_eq!(call.method(), "GetTable");
            let request: GetTableRequest =
                call.next_request().await.expect("the fake server answers");
            call.log(format!(
                "GetTable {}.{}",
                request.dataset_id, request.table_id
            ));
            call.reply(&Table {
                id: format!(
                    "{}:{}.{}",
                    request.project_id, request.dataset_id, request.table_id
                ),
                ..Default::default()
            });
        })
        .await;
        let table = fake
            .db
            .table_client()
            .get_table(GetTableRequest {
                project_id: "acme-prod".into(),
                dataset_id: "shop".into(),
                table_id: "orders".into(),
                ..Default::default()
            })
            .await
            .expect("the fake server answers")
            .into_inner();
        assert_eq!(table.id, "acme-prod:shop.orders");
        assert_eq!(fake.calls(), ["GetTable shop.orders"]);
    }

    #[tokio::test]
    async fn server_stream_sends_messages_then_a_status() {
        let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
            let request: ReadRowsRequest =
                call.next_request().await.expect("the fake server answers");
            call.log(format!(
                "ReadRows {} at {}",
                request.read_stream, request.offset
            ));
            for row_count in [1, 2] {
                call.send(&ReadRowsResponse {
                    row_count,
                    ..Default::default()
                });
            }
            call.fail(Code::Unavailable, "backend went away: 50%");
        })
        .await;
        let mut responses = fake
            .db
            .read_client()
            .read_rows(ReadRowsRequest {
                read_stream: "s0".into(),
                offset: 5,
                ..Default::default()
            })
            .await
            .expect("the fake server answers")
            .into_inner();
        let mut counts = Vec::new();
        let status = loop {
            match responses.next().await {
                Some(Ok(response)) => counts.push(response.row_count),
                Some(Err(status)) => break status,
                None => panic!("the stream must end with the status"),
            }
        };
        assert_eq!(counts, [1, 2]);
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), "backend went away: 50%");
        assert_eq!(fake.calls(), ["ReadRows s0 at 5"]);
    }

    #[tokio::test]
    async fn bidi_stream_answers_each_request_in_turn() {
        let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
            call.open();
            while let Some(request) = call.next_request::<AppendRowsRequest>().await {
                call.log(format!("AppendRows {}", request.trace_id));
                call.send(&AppendRowsResponse {
                    write_stream: request.trace_id,
                    ..Default::default()
                });
            }
            call.log("AppendRows closed");
            call.finish();
        })
        .await;
        let (requests, receiver) = futures::channel::mpsc::unbounded();
        let mut responses = fake
            .db
            .write_client()
            .append_rows(receiver)
            .await
            .expect("the fake server answers")
            .into_inner();
        let mut answered = Vec::new();
        for trace_id in ["a", "b", "c"] {
            requests
                .unbounded_send(AppendRowsRequest {
                    trace_id: trace_id.into(),
                    ..Default::default()
                })
                .expect("the fake server answers");
            answered.push(
                responses
                    .next()
                    .await
                    .expect("the fake server answers")
                    .expect("the fake server answers")
                    .write_stream,
            );
        }
        drop(requests);
        assert!(
            responses.next().await.is_none(),
            "finish ends the stream cleanly"
        );
        assert_eq!(answered, ["a", "b", "c"]);
        assert_eq!(
            fake.calls(),
            [
                "AppendRows a",
                "AppendRows b",
                "AppendRows c",
                "AppendRows closed"
            ]
        );
    }

    #[tokio::test]
    async fn dropped_connection_is_a_retryable_transport_error() {
        let fake = FakeBigQuery::start(|call: FakeCall| async move {
            call.log("GetTable dropped");
            call.drop_connection().await;
        })
        .await;
        let status = fake
            .db
            .table_client()
            .get_table(GetTableRequest::default())
            .await
            .expect_err("the call must fail");
        let err = BigQueryError::from(status);
        assert!(err.retry_possible(), "{err}");
        assert_eq!(fake.calls(), ["GetTable dropped"]);
    }

    #[tokio::test]
    async fn failure_before_any_message_is_visible_at_the_call() {
        let fake = FakeBigQuery::start(|call: FakeCall| async move {
            call.fail(Code::NotFound, "Not found: Table p:shop.orders");
        })
        .await;
        let status = fake
            .db
            .read_client()
            .read_rows(ReadRowsRequest::default())
            .await
            .expect_err("the call must fail");
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), "Not found: Table p:shop.orders");
    }

    #[tokio::test]
    async fn hung_call_stays_open_until_the_client_gives_up() {
        let fake = FakeBigQuery::start(|call: FakeCall| async move {
            call.log("GetTable hung");
            call.hang().await;
        })
        .await;
        let mut client = fake.db.table_client();
        let pending =
            tokio::spawn(async move { client.get_table(GetTableRequest::default()).await });
        fake.wait_for_calls(1).await;
        let waited = tokio::time::timeout(std::time::Duration::from_millis(200), pending).await;
        assert!(waited.is_err(), "the call must still be in flight");
    }

    #[tokio::test]
    async fn an_endpoint_with_a_trailing_slash_or_a_path_reaches_the_service() {
        let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
            let request: GetTableRequest =
                call.next_request().await.expect("the fake server answers");
            call.log(format!("GetTable {}", request.table_id));
            call.reply(&Table::default());
        })
        .await;
        let endpoint = fake.db.options().effective_bigquery_api_url();
        assert!(endpoint.as_str().ends_with('/'), "{endpoint}");
        let options = fake
            .db
            .options()
            .clone()
            .with_bigquery_api_url(endpoint.join("prefix/").expect("a relative path joins"));
        let db = BigQueryDb::with_options_token_source(
            options,
            Vec::new(),
            gcloud_sdk::TokenSourceType::ExternalSource(Box::new(FakeTokenSource)),
        )
        .await
        .expect("a client for the fake server");
        for (db, table_id) in [(&fake.db, "t1"), (&db, "t2")] {
            db.table_client()
                .get_table(GetTableRequest {
                    table_id: table_id.into(),
                    ..Default::default()
                })
                .await
                .expect("the fake server answers");
        }
        assert_eq!(fake.calls(), ["GetTable t1", "GetTable t2"]);
    }
}
