//! Runs code that queries and writes BigQuery against the fake from `bigquery::testing`: a
//! scripted query, a write whose rows are read back, a failure the client retries past, and a
//! lost append acknowledgement on the default stream and on an exactly-once write.
//!
//! Needs no credentials and no network. Run with `cargo run --example testing-fake`.

use bigquery::testing::{BigQueryFake, BigQueryFakeCode, BigQueryFakeFault, BigQueryFakeRpc};
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    id: i64,
    customer: String,
    total: f64,
}

const SHOP: BigQueryDatasetId = BigQueryDatasetId::from_static("shop");
const ORDERS: BigQueryTableId = BigQueryTableId::from_static("orders");
const ORDERS_OF: &str = "SELECT id, customer, total FROM shop.orders WHERE customer = @customer";

// The code under test: it takes a `BigQueryDb` and knows nothing of the fake.

async fn orders_of(db: &BigQueryDb, customer: &str) -> BigQueryResult<Vec<Order>> {
    db.fluent()
        .query(ORDERS_OF)
        .param("customer", customer)
        .obj()
        .query()
        .await
}

async fn save(db: &BigQueryDb, orders: &[Order]) -> BigQueryResult<BigQueryWriteSummary> {
    db.fluent()
        .insert()
        .into(SHOP.table(ORDERS))
        .objects(orders)
        .execute()
        .await
}

async fn save_exactly_once(
    db: &BigQueryDb,
    orders: &[Order],
) -> BigQueryResult<BigQueryWriteSummary> {
    db.fluent()
        .insert()
        .into(SHOP.table(ORDERS))
        .objects(orders)
        .exactly_once()
        .execute()
        .await
}

fn order(id: i64, customer: &str, total: f64) -> Order {
    Order {
        id,
        customer: customer.to_string(),
        total,
    }
}

async fn scripted_query() -> BigQueryResult<()> {
    let fake = BigQueryFake::start().await?;
    let alice = vec![order(1, "Alice", 120.0), order(3, "Alice", 15.25)];
    let rule = fake
        .when_query_match(ORDERS_OF)
        .param("customer", "Alice")
        .returns_rows(|columns| columns.from_type::<Order>(), &alice)?;

    let found = orders_of(fake.db(), "Alice").await?;

    assert_eq!(found, alice);
    assert_eq!(rule.calls(), 1);
    println!("Query answered by its rule: {found:?}");
    Ok(())
}

async fn captured_write() -> BigQueryResult<()> {
    let fake = BigQueryFake::start().await?;
    fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
        .create()?;
    let orders = vec![order(1, "Alice", 120.0), order(2, "Bob", 80.5)];

    let summary = save(fake.db(), &orders).await?;

    let written: Vec<Order> = fake.rows(SHOP.table(ORDERS))?;
    assert_eq!(summary.rows_written, 2);
    assert_eq!(written, orders);
    println!("Rows the write appended: {written:?}");
    Ok(())
}

async fn retried_failure() -> BigQueryResult<()> {
    let fake = BigQueryFake::start().await?;
    let alice = vec![order(1, "Alice", 120.0)];
    let lost = fake
        .when_query_match(ORDERS_OF)
        .times(1)
        .fails(BigQueryFakeFault::status(
            BigQueryFakeCode::Unavailable,
            "backend went away",
        ))?;
    let answer = fake
        .when_query_match(ORDERS_OF)
        .returns_rows(|columns| columns.from_type::<Order>(), &alice)?;

    let found = orders_of(fake.db(), "Alice").await?;

    assert_eq!(found, alice);
    assert_eq!((lost.calls(), answer.calls()), (1, 1));
    println!("Query retried past one UNAVAILABLE: {found:?}");
    Ok(())
}

async fn lost_append_acknowledgement() -> BigQueryResult<()> {
    let orders = vec![order(1, "Alice", 120.0), order(2, "Bob", 80.5)];

    let fake = BigQueryFake::start().await?;
    fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
        .create()?;
    fake.when_fault(BigQueryFakeRpc::AppendRows)
        .times(1)
        .fails(BigQueryFakeFault::ConnectionDropped)?;
    save(fake.db(), &orders).await?;
    let at_least_once: Vec<Order> = fake.rows(SHOP.table(ORDERS))?;
    assert_eq!(at_least_once.len(), 2 * orders.len());
    println!(
        "Default stream after a lost acknowledgement: {} rows for {} written",
        at_least_once.len(),
        orders.len()
    );

    let fake = BigQueryFake::start().await?;
    fake.table(SHOP.table(ORDERS), |columns| columns.from_type::<Order>())
        .create()?;
    fake.when_fault(BigQueryFakeRpc::AppendRows)
        .times(1)
        .fails(BigQueryFakeFault::ConnectionDropped)?;
    save_exactly_once(fake.db(), &orders).await?;
    let exactly_once: Vec<Order> = fake.rows(SHOP.table(ORDERS))?;
    assert_eq!(exactly_once, orders);
    println!(
        "Exactly-once write after a lost acknowledgement: {} rows for {} written",
        exactly_once.len(),
        orders.len()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> BigQueryResult<()> {
    scripted_query().await?;
    captured_write().await?;
    retried_failure().await?;
    lost_append_acknowledgement().await?;
    Ok(())
}
