use super::*;
use crate::errors::BigQueryError;
use crate::sql::tests::{injection_corpus, lex_string};
use crate::BigQueryTimestamp;

fn f() -> BigQueryFilterBuilder {
    BigQueryFilterBuilder::new()
}

fn render(filter: Option<BigQueryFilter>) -> String {
    filter
        .expect("a filter")
        .into_row_restriction()
        .expect("a valid filter")
}

fn failure(filter: Option<BigQueryFilter>) -> BigQueryError {
    match filter.expect("a filter").into_row_restriction() {
        Ok(sql) => panic!("expected a refused filter, got {sql:.80}"),
        Err(failure) => failure,
    }
}

#[test]
fn comparisons_put_a_quoted_column_before_a_literal() {
    let f = f();
    assert_eq!(render(f.field("n").eq(1)), "`n` = 1");
    assert_eq!(render(f.field("n").neq(1)), "`n` != 1");
    assert_eq!(render(f.field("n").lt(1)), "`n` < 1");
    assert_eq!(render(f.field("n").le(1)), "`n` <= 1");
    assert_eq!(render(f.field("n").gt(1.5)), "`n` > 1.5");
    assert_eq!(render(f.field("n").ge(-1)), "`n` >= -1");
    assert_eq!(
        render(f.field("home.county").eq("Skåne")),
        "`home`.`county` = 'Skåne'"
    );
}

#[test]
fn null_checks_and_lists_render_their_operators() {
    let f = f();
    assert_eq!(render(f.field("n").is_null()), "`n` IS NULL");
    assert_eq!(render(f.field("n").is_not_null()), "`n` IS NOT NULL");
    assert_eq!(render(f.field("n").is_in([1, 2])), "`n` IN (1, 2)");
    assert_eq!(
        render(f.field("s").is_not_in(["a", "b"])),
        "`s` NOT IN ('a', 'b')"
    );
    assert_eq!(render(f.field("n").is_in(Vec::<i64>::new())), "FALSE");
    assert_eq!(render(f.field("n").is_not_in(Vec::<i64>::new())), "TRUE");
}

#[test]
fn nested_groups_keep_their_precedence() {
    let f = f();
    let a = || f.field("a").eq(1);
    let b = || f.field("b").eq(2);
    let c = || f.field("c").eq(3);
    assert_eq!(
        render(f.for_all([a(), f.for_any([b(), c()]), f.not(f.field("d").eq(4))])),
        "`a` = 1 AND (`b` = 2 OR `c` = 3) AND NOT (`d` = 4)"
    );
    assert_eq!(
        render(f.for_any([f.for_all([a(), b()]), c()])),
        "(`a` = 1 AND `b` = 2) OR `c` = 3"
    );
    assert_eq!(
        render(f.not(f.for_any([a(), b()]))),
        "NOT (`a` = 1 OR `b` = 2)"
    );
    assert_eq!(
        render(f.for_all([f.not(f.not(a())), f.for_all([b(), c()])])),
        "NOT (NOT (`a` = 1)) AND (`b` = 2 AND `c` = 3)"
    );
}

#[test]
fn none_entries_are_dropped_and_a_single_condition_stands_alone() {
    let f = f();
    let year: Option<i64> = None;
    assert_eq!(
        render(f.for_all([
            None,
            f.field("a").eq(1),
            year.and_then(|y| f.field("year").ge(y)),
        ])),
        "`a` = 1"
    );
    assert_eq!(
        render(f.for_any([f.field("a").eq(1), None, f.field("b").eq(2)])),
        "`a` = 1 OR `b` = 2"
    );
}

#[test]
fn no_conditions_mean_no_filter() {
    let f = f();
    assert!(f.for_all(Vec::<Option<BigQueryFilter>>::new()).is_none());
    assert!(f.for_any([None::<BigQueryFilter>, None]).is_none());
    assert!(f.not(None::<BigQueryFilter>).is_none());
    assert!(f.for_all([f.for_any([None::<BigQueryFilter>])]).is_none());
}

#[test]
fn hostile_values_stay_one_literal() {
    let f = f();
    for payload in injection_corpus().into_iter().filter(|p| p.len() < 1 << 19) {
        let sql = render(f.field("s").eq(payload.as_str()));
        let literal = sql.strip_prefix("`s` = ").expect("the column and operator");
        assert_eq!(lex_string(literal), payload, "{payload:?}");

        let sql = render(f.field("s").is_in([payload.as_str(), "x"]));
        let list = sql
            .strip_prefix("`s` IN (")
            .and_then(|s| s.strip_suffix(", 'x')"))
            .expect("the list");
        assert_eq!(lex_string(list), payload, "{payload:?}");
    }
}

#[test]
fn wrappers_compare_as_their_bigquery_type() {
    let ts: jiff::Timestamp = "2026-10-04T12:00:00Z".parse().expect("valid");
    assert_eq!(
        render(f().field("at").ge(BigQueryTimestamp(ts))),
        "`at` >= TIMESTAMP '2026-10-04 12:00:00+00:00'"
    );
    assert_eq!(
        render(f().field("at").ge(ts)),
        "`at` >= '2026-10-04T12:00:00Z'",
        "a plain jiff value is a STRING, which BigQuery coerces"
    );
}

#[test]
fn null_values_are_refused_with_a_pointer_to_is_null() {
    let f = f();
    for err in [
        failure(f.field("n").eq(None::<i64>)),
        failure(f.field("n").is_in([Some(1), None])),
    ] {
        match err {
            BigQueryError::InvalidParametersError(e) => {
                assert_eq!(e.public.field, "n");
                assert!(e.public.error.contains("is_null"), "{}", e.public.error);
            }
            other => panic!("expected InvalidParametersError, got {other:?}"),
        }
    }
}

#[test]
fn an_invalid_column_fails_the_whole_filter() {
    let f = f();
    let err = failure(f.for_all([f.field("ok").eq(1), f.field("a\nb").eq(2)]));
    match err {
        BigQueryError::InvalidParametersError(e) => assert_eq!(e.public.field, "a\nb"),
        other => panic!("expected InvalidParametersError, got {other:?}"),
    }
    assert!(matches!(
        failure(f.not(f.field("a..b").is_null())),
        BigQueryError::InvalidParametersError(_)
    ));
}

#[test]
fn a_hostile_column_name_stays_one_identifier() {
    assert_eq!(
        render(f().field("a` = 1 OR `b").eq(2)),
        "`a\\` = 1 OR \\`b` = 2"
    );
}
