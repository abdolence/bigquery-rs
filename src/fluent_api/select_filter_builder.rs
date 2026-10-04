//! A typed row filter for table reads, rendered into the read session's `row_restriction`.
//!
//! The Storage Read API takes the filter as GoogleSQL text and has no query parameters, so
//! every value is written into the text as a literal. The builder writes each value through
//! the crate's literal renderer, which escapes it so that it cannot end its own token, and
//! each column name as an escaped, backtick-quoted identifier.
//!
//! Start from [`BigQuerySelectBuilder::filter`](crate::BigQuerySelectBuilder::filter).

use crate::errors::{BigQueryInvalidParametersError, BigQueryInvalidParametersPublicDetails};
use crate::query::{literal_of, ParamFailure};
use crate::sql::{ColumnPath, SqlLiteral};
use serde::Serialize;

/// Builds row filters: the closure given to
/// [`filter`](crate::BigQuerySelectBuilder::filter) receives one.
///
/// Combine conditions with [`for_all`](Self::for_all) and [`for_any`](Self::for_any), which
/// skip `None` entries so that an optional condition can be written inline:
///
/// ```rust
/// # use bigquery::*;
/// # fn build(f: BigQueryFilterBuilder, min_year: Option<i64>) -> Option<BigQueryFilter> {
/// #[derive(serde::Deserialize)]
/// struct Person {
///     county: String,
///     year: i64,
/// }
///
/// f.for_all([
///     f.field(path!(Person::county)).eq("Skåne"),
///     min_year.and_then(|year| f.field(path!(Person::year)).ge(year)),
/// ])
/// # }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct BigQueryFilterBuilder;

impl BigQueryFilterBuilder {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Matches the rows that match every condition (`AND`). `None` entries are skipped; a
    /// single remaining condition is returned as it is, and none at all is `None`, which
    /// filters nothing.
    pub fn for_all<I>(&self, conditions: I) -> Option<BigQueryFilter>
    where
        I: IntoIterator,
        I::Item: BigQueryFilterExpr,
    {
        BigQueryFilter::combine(conditions, FilterExpr::All)
    }

    /// Matches the rows that match at least one condition (`OR`). `None` entries are skipped
    /// as in [`for_all`](Self::for_all).
    pub fn for_any<I>(&self, conditions: I) -> Option<BigQueryFilter>
    where
        I: IntoIterator,
        I::Item: BigQueryFilterExpr,
    {
        BigQueryFilter::combine(conditions, FilterExpr::Any)
    }

    /// Matches the rows the condition does not match (`NOT`). A `None` condition stays
    /// `None`.
    ///
    /// GoogleSQL's three-valued logic applies: a row where the condition is NULL, such as
    /// `n = 1` on a NULL `n`, matches neither the condition nor its negation.
    pub fn not<F: BigQueryFilterExpr>(&self, condition: F) -> Option<BigQueryFilter> {
        condition
            .build_filter()
            .map(|f| BigQueryFilter(f.0.map(|e| FilterExpr::Not(Box::new(e)))))
    }

    /// Targets a column for a condition. Name a STRUCT subfield as `rec.field`;
    /// [`path!`](crate::path!) builds the name from a struct's field. A path with an empty
    /// segment or a control character fails the read's terminal with
    /// [`InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError)
    /// before any request; BigQuery checks the rest of its column name rules itself.
    pub fn field<S: AsRef<str>>(&self, column: S) -> BigQueryFilterFieldExpr {
        BigQueryFilterFieldExpr {
            column: column.as_ref().parse().map_err(ParamFailure::Invalid),
        }
    }
}

/// A row filter from [`BigQueryFilterBuilder`]. It holds the first error any of its parts
/// met, which the read's terminal returns.
#[derive(Clone, Debug)]
pub struct BigQueryFilter(Result<FilterExpr, ParamFailure>);

impl BigQueryFilter {
    fn combine<I>(conditions: I, op: fn(Vec<FilterExpr>) -> FilterExpr) -> Option<Self>
    where
        I: IntoIterator,
        I::Item: BigQueryFilterExpr,
    {
        let mut parts: Vec<BigQueryFilter> = conditions
            .into_iter()
            .filter_map(BigQueryFilterExpr::build_filter)
            .collect();
        if parts.len() <= 1 {
            return parts.pop();
        }
        let exprs = parts
            .into_iter()
            .map(|p| p.0)
            .collect::<Result<Vec<_>, _>>();
        Some(BigQueryFilter(exprs.map(op)))
    }

    /// The `row_restriction` text.
    pub(crate) fn into_row_restriction(self) -> Result<String, ParamFailure> {
        Ok(self.0?.to_string())
    }
}

/// What [`BigQueryFilterBuilder::for_all`], [`for_any`](BigQueryFilterBuilder::for_any) and
/// [`not`](BigQueryFilterBuilder::not) take: a filter, or an optional one, where `None`
/// stands for no condition.
pub trait BigQueryFilterExpr {
    /// The filter, or `None` for no condition.
    fn build_filter(self) -> Option<BigQueryFilter>;
}

impl BigQueryFilterExpr for BigQueryFilter {
    fn build_filter(self) -> Option<BigQueryFilter> {
        Some(self)
    }
}

impl<F: BigQueryFilterExpr> BigQueryFilterExpr for Option<F> {
    fn build_filter(self) -> Option<BigQueryFilter> {
        self.and_then(BigQueryFilterExpr::build_filter)
    }
}

/// A column targeted by [`BigQueryFilterBuilder::field`]: pick the condition.
///
/// Values are taken as any `Serialize` and written as literals of the BigQuery type their
/// serde form maps to, the same mapping query parameters use: a string is a STRING, an
/// integer an INT64, a float a FLOAT64, [`BigQueryTimestamp`](crate::BigQueryTimestamp) a
/// TIMESTAMP, [`BigQueryDecimal`](crate::BigQueryDecimal) a NUMERIC or BIGNUMERIC, a sequence
/// an ARRAY and a struct a STRUCT. A NULL value is refused: `n = NULL` matches no row, so
/// compare with [`is_null`](Self::is_null) instead.
#[derive(Clone, Debug)]
pub struct BigQueryFilterFieldExpr {
    column: Result<ColumnPath, ParamFailure>,
}

impl BigQueryFilterFieldExpr {
    fn compare<V: Serialize + ?Sized>(self, op: CompareOp, value: &V) -> Option<BigQueryFilter> {
        let expr = self.column.and_then(|column| {
            let value = literal_of(&column.to_string(), value)?.ok_or_else(|| {
                ParamFailure::Invalid(BigQueryInvalidParametersError::new(
                    BigQueryInvalidParametersPublicDetails::new(
                        column.to_string(),
                        format!(
                            "`{column} {} NULL` matches no row; use is_null or is_not_null",
                            op.sql()
                        ),
                    ),
                ))
            })?;
            Ok(FilterExpr::Compare { column, op, value })
        });
        Some(BigQueryFilter(expr))
    }

    fn membership<I>(self, values: I, negated: bool) -> Option<BigQueryFilter>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        let expr = self.column.and_then(|column| {
            let name = column.to_string();
            let values = values
                .into_iter()
                .map(|v| {
                    literal_of(&name, &v)?.ok_or_else(|| {
                        ParamFailure::Invalid(BigQueryInvalidParametersError::new(
                            BigQueryInvalidParametersPublicDetails::new(
                                name.clone(),
                                format!(
                                    "a NULL in the list of `{name} IN (..)` matches no row; \
                                     combine with is_null instead"
                                ),
                            ),
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            Ok(FilterExpr::In {
                column,
                values,
                negated,
            })
        });
        Some(BigQueryFilter(expr))
    }

    /// `column = value`.
    pub fn eq<V: Serialize>(self, value: V) -> Option<BigQueryFilter> {
        self.compare(CompareOp::Eq, &value)
    }

    /// `column != value`. A row where the column is NULL does not match.
    pub fn neq<V: Serialize>(self, value: V) -> Option<BigQueryFilter> {
        self.compare(CompareOp::Neq, &value)
    }

    /// `column < value`.
    pub fn lt<V: Serialize>(self, value: V) -> Option<BigQueryFilter> {
        self.compare(CompareOp::Lt, &value)
    }

    /// `column <= value`.
    pub fn le<V: Serialize>(self, value: V) -> Option<BigQueryFilter> {
        self.compare(CompareOp::Le, &value)
    }

    /// `column > value`.
    pub fn gt<V: Serialize>(self, value: V) -> Option<BigQueryFilter> {
        self.compare(CompareOp::Gt, &value)
    }

    /// `column >= value`.
    pub fn ge<V: Serialize>(self, value: V) -> Option<BigQueryFilter> {
        self.compare(CompareOp::Ge, &value)
    }

    /// `column IS NULL`.
    pub fn is_null(self) -> Option<BigQueryFilter> {
        Some(BigQueryFilter(self.column.map(|column| {
            FilterExpr::IsNull {
                column,
                negated: false,
            }
        })))
    }

    /// `column IS NOT NULL`.
    pub fn is_not_null(self) -> Option<BigQueryFilter> {
        Some(BigQueryFilter(self.column.map(|column| {
            FilterExpr::IsNull {
                column,
                negated: true,
            }
        })))
    }

    /// `column IN (values..)`. An empty list matches no row.
    pub fn is_in<I>(self, values: I) -> Option<BigQueryFilter>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        self.membership(values, false)
    }

    /// `column NOT IN (values..)`. An empty list matches every row; otherwise a row where
    /// the column is NULL does not match.
    pub fn is_not_in<I>(self, values: I) -> Option<BigQueryFilter>
    where
        I: IntoIterator,
        I::Item: Serialize,
    {
        self.membership(values, true)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CompareOp {
    fn sql(self) -> &'static str {
        match self {
            CompareOp::Eq => "=",
            CompareOp::Neq => "!=",
            CompareOp::Lt => "<",
            CompareOp::Le => "<=",
            CompareOp::Gt => ">",
            CompareOp::Ge => ">=",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum FilterExpr {
    Compare {
        column: ColumnPath,
        op: CompareOp,
        value: SqlLiteral,
    },
    IsNull {
        column: ColumnPath,
        negated: bool,
    },
    In {
        column: ColumnPath,
        values: Vec<SqlLiteral>,
        negated: bool,
    },
    All(Vec<FilterExpr>),
    Any(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
}

impl FilterExpr {
    /// Writes a member of an `AND` or `OR` list. Comparisons, null checks, lists and `NOT`
    /// all bind tighter than `AND` and `OR`, so only a nested list needs parentheses.
    fn fmt_member(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterExpr::All(_) | FilterExpr::Any(_) => write!(f, "({self})"),
            _ => write!(f, "{self}"),
        }
    }

    fn fmt_list(
        f: &mut std::fmt::Formatter<'_>,
        members: &[FilterExpr],
        op: &str,
    ) -> std::fmt::Result {
        for (i, member) in members.iter().enumerate() {
            if i > 0 {
                write!(f, " {op} ")?;
            }
            member.fmt_member(f)?;
        }
        Ok(())
    }
}

impl std::fmt::Display for FilterExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterExpr::Compare { column, op, value } => {
                write!(f, "{} {} {value}", column.sql(), op.sql())
            }
            FilterExpr::IsNull { column, negated } => {
                let not = if *negated { " NOT" } else { "" };
                write!(f, "{} IS{not} NULL", column.sql())
            }
            // `x IN ()` is not GoogleSQL; an empty list is the truth value it would have.
            FilterExpr::In {
                values, negated, ..
            } if values.is_empty() => {
                write!(f, "{}", SqlLiteral::bool(*negated))
            }
            FilterExpr::In {
                column,
                values,
                negated,
            } => {
                let not = if *negated { " NOT" } else { "" };
                write!(f, "{}{not} IN (", column.sql())?;
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{value}")?;
                }
                f.write_str(")")
            }
            FilterExpr::All(members) => FilterExpr::fmt_list(f, members, "AND"),
            FilterExpr::Any(members) => FilterExpr::fmt_list(f, members, "OR"),
            FilterExpr::Not(inner) => write!(f, "NOT ({inner})"),
        }
    }
}

#[cfg(test)]
mod tests;
