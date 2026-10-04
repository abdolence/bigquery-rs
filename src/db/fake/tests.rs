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
        let request: GetTableRequest = call.next_request().await.expect("the fake server answers");
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
        let request: ReadRowsRequest = call.next_request().await.expect("the fake server answers");
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
    let pending = tokio::spawn(async move { client.get_table(GetTableRequest::default()).await });
    fake.wait_for_calls(1).await;
    let waited = tokio::time::timeout(std::time::Duration::from_millis(200), pending).await;
    assert!(waited.is_err(), "the call must still be in flight");
}

#[tokio::test]
async fn an_endpoint_with_a_trailing_slash_or_a_path_reaches_the_service() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        let request: GetTableRequest = call.next_request().await.expect("the fake server answers");
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
