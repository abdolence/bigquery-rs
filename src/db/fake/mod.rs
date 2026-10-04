//! A fake BigQuery gRPC server for driving a real [`BigQueryDb`] against a controlled backend.
//!
//! It speaks raw HTTP/2, so a handler sees each request message as it arrives and answers in
//! any shape gRPC allows: a unary reply, a server stream, a bidi exchange one request at a
//! time, a status at any point, a dropped connection or a call that never answers. The
//! per-API answers live in the sibling `read`, `write` and `query` modules.

mod admin;
mod query;
mod read;
pub(crate) mod spans;
mod table;
mod write;

use crate::{BigQueryDb, BigQueryDbOptions, BigQueryEndpoint};
use futures::future::BoxFuture;
use gcloud_sdk::prost::Message;
use gcloud_sdk::tonic::Code;
use h2::server::SendResponse;
use h2::{RecvStream, SendStream};
use hyper::body::Bytes;
use hyper::header::HeaderValue;
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Notify};

type Handler = dyn Fn(FakeCall) -> BoxFuture<'static, ()> + Send + Sync;

/// A running fake server and a [`BigQueryDb`] whose v2 and Storage endpoints both point at it.
///
/// The handler runs once per RPC, on its own task, and owns the call until it answers. It
/// records what it saw with [`FakeCall::log`], so a test can assert a whole RPC sequence with
/// one comparison of [`FakeBigQuery::calls`].
pub(crate) struct FakeBigQuery {
    pub db: BigQueryDb,
    calls: Arc<watch::Sender<Vec<String>>>,
}

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
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free loopback port for the fake server");
        let endpoint: BigQueryEndpoint = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("a bound listener has a local address")
        )
        .parse()
        .expect("a loopback URL is an endpoint");
        let handler: Arc<Handler> = Arc::new(move |call| Box::pin(handler(call)));
        let calls = Arc::new(watch::Sender::new(Vec::new()));
        let accepted = calls.clone();
        // Detached: #[tokio::test] drops every spawned task, this one included, at test end.
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve_connection(socket, handler.clone(), accepted.clone()));
            }
        });
        let options = BigQueryDbOptions::new("fake-project".into())
            .with_max_retries(max_retries)
            .with_bigquery_api_url(endpoint.clone())
            .with_bigquery_storage_api_url(endpoint);
        let db = BigQueryDb::with_options_token_source(
            options,
            Vec::new(),
            gcloud_sdk::TokenSourceType::ExternalSource(Box::new(FakeTokenSource)),
        )
        .await
        .expect("a client for the fake server");
        Self { db, calls }
    }

    /// Every line the handlers logged so far, in the order they logged them.
    pub fn calls(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }

    /// Resolves once at least `count` lines have been logged.
    pub async fn wait_for_calls(&self, count: usize) {
        self.calls
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
    calls: Arc<watch::Sender<Vec<String>>>,
    close: Arc<Notify>,
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

    /// The next request message, decoded.
    ///
    /// # Panics
    /// If the bytes are not an `M`, which is a test bug.
    pub async fn next_request<M: Message + Default>(&mut self) -> Option<M> {
        let bytes = self.next_message().await?;
        Some(M::decode(bytes.as_slice()).expect("the request decodes as the handler's type"))
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

/// Serves one client connection until the client closes it or a handler drops it.
async fn serve_connection(
    socket: TcpStream,
    handler: Arc<Handler>,
    calls: Arc<watch::Sender<Vec<String>>>,
) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    let close = Arc::new(Notify::new());
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
                calls: calls.clone(),
                close: close.clone(),
            };
            tokio::spawn(handler(call));
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
mod tests;
