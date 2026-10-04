use super::*;
use crate::write::encoder::tests::field;
use crate::{BigQueryFieldMode, BigQueryFieldType, BigQueryTableSchema};
use gcloud_sdk::prost::Message;
use proptest::prelude::*;
use std::sync::Arc;

const STREAM: &str =
    "projects/p/datasets/d/tables/t/streams/Cic2NjQ2ZjZkNjE2OTZlEhAKCgoKCgoKCgoKCgoK";

fn plan(columns: usize) -> Arc<WritePlan> {
    let fields = (0..columns)
        .map(|i| {
            field(
                &format!("column_with_a_long_name_{i}"),
                BigQueryFieldType::String { max_length: None },
                BigQueryFieldMode::Nullable,
            )
        })
        .collect();
    Arc::new(WritePlan::new(&BigQueryTableSchema { fields }, false))
}

fn target() -> RequestTarget {
    RequestTarget {
        write_stream: STREAM.to_string(),
        trace_id: Some("bigquery-rs:test".into()),
        missing_value: None,
    }
}

/// Batches `rows` as the writer does: a row that does not fit closes the open batch.
fn batch_all(batcher: &mut Batcher, rows: &[usize]) -> Result<Vec<Batch>, CodecError> {
    let mut sealed = Vec::new();
    for &len in rows {
        let cost = batcher.cost(len)?;
        if !batcher.fits(cost) {
            sealed.extend(batcher.seal());
        }
        batcher.push(vec![7u8; len], cost, Instant::now());
        if batcher.is_full() {
            sealed.extend(batcher.seal());
        }
    }
    sealed.extend(batcher.seal());
    Ok(sealed)
}

proptest! {
    #[test]
    fn requests_never_exceed_the_budget(
        max_request_bytes in 2_000usize..40_000,
        columns in 1usize..40,
        rows in proptest::collection::vec(0usize..3_000, 1..200),
        max_rows in proptest::option::of(1usize..20),
    ) {
        let plan = plan(columns);
        let target = target();
        let mut batcher = Batcher::new(plan.clone(), &target, max_request_bytes, max_rows);
        let capacity = batcher.capacity();
        let fitting: Vec<usize> = rows.iter().copied().filter(|&len| len + 8 <= capacity).collect();
        prop_assume!(!fitting.is_empty());
        let batches = batch_all(&mut batcher, &fitting).expect("every row fits");
        let mut written = Vec::new();
        for (k, batch) in batches.iter().enumerate() {
            prop_assert_eq!(batch.index, k as u64);
            prop_assert_eq!(batch.first_row, written.len() as u64);
            if let Some(max_rows) = max_rows {
                prop_assert!(batch.rows.len() <= max_rows);
            }
            let request = append_request(
                &target,
                Some(i64::MAX),
                Some(&plan),
                batch.rows.clone(),
            );
            let size = request.encoded_len();
            prop_assert!(size <= max_request_bytes, "{} > {}", size, max_request_bytes);
            prop_assert_eq!(size - batch.bytes <= batcher.overhead(), true);
            written.extend(batch.rows.iter().map(Vec::len));
        }
        prop_assert_eq!(written, fitting.clone());
        // No batch was closed early: the next batch's first row did not fit it.
        for pair in batches.windows(2) {
            let next = pair[1].rows[0].len();
            let full_by_rows = max_rows.is_some_and(|m| pair[0].rows.len() >= m);
            let cost = 1 + varint_len(next as u64) + next;
            prop_assert!(full_by_rows || pair[0].bytes + cost > capacity);
        }
    }
}

#[test]
fn a_row_larger_than_the_budget_is_row_too_large() {
    let target = target();
    let batcher = Batcher::new(plan(3), &target, 1_000, None);
    let capacity = batcher.capacity();
    let fits = capacity - 1 - varint_len(capacity as u64);
    assert!(batcher.cost(fits).is_ok());
    let err = batcher
        .cost(fits + 1)
        .expect_err("one byte over the budget");
    assert_eq!(err.kind(), BigQueryCodecErrorKind::RowTooLarge);
    let err = batcher.cost(10_000).expect_err("far over the budget");
    assert_eq!(err.kind(), BigQueryCodecErrorKind::RowTooLarge);
}

#[test]
fn descriptor_size_is_reserved_in_every_request() {
    let target = target();
    let wide = plan(200);
    let budget = 30_000;
    let mut batcher = Batcher::new(wide.clone(), &target, budget, None);
    let descriptor = wide.descriptor().encoded_len();
    assert!(
        batcher.overhead() > descriptor,
        "{} <= {descriptor}",
        batcher.overhead()
    );
    let batches = batch_all(&mut batcher, &[500; 200]).expect("rows fit");
    assert!(batches.len() > 2);
    // Any batch may be the first one on a new connection, so each must fit with the
    // descriptor even when it was first sent without one.
    for batch in &batches {
        let with = append_request(&target, Some(1), Some(&wide), batch.rows.clone());
        let without = append_request(&target, Some(1), None, batch.rows.clone());
        assert!(with.encoded_len() <= budget, "{}", with.encoded_len());
        assert!(without.encoded_len() + descriptor <= budget);
        assert!(writer_schema_is_absent(&without));
    }
}

/// Whether a request carries no writer schema, as every request after the first on a
/// connection with an unchanged plan.
fn writer_schema_is_absent(request: &AppendRowsRequest) -> bool {
    match &request.rows {
        Some(append_rows_request::Rows::ProtoRows(data)) => data.writer_schema.is_none(),
        _ => false,
    }
}
