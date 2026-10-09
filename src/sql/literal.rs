use super::ident::{escape_into, quote_identifier};
use crate::errors::BigQueryCodecErrorKind;
use crate::query::{bytes_from_base64, float_from_text};
use crate::types::error::CodecError;
use crate::BigQueryRangeElementType;
use gcloud_sdk::google::cloud::bigquery::v2::{QueryParameterType, QueryParameterValue};
use std::fmt::{Display, Formatter};

/// The kinds whose literal is a keyword before a string literal holding the value's text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TextLiteralKind {
    Numeric,
    BigNumeric,
    Date,
    Time,
    DateTime,
    Timestamp,
    Json,
}

impl TextLiteralKind {
    fn keyword(self) -> &'static str {
        match self {
            TextLiteralKind::Numeric => "NUMERIC",
            TextLiteralKind::BigNumeric => "BIGNUMERIC",
            TextLiteralKind::Date => "DATE",
            TextLiteralKind::Time => "TIME",
            TextLiteralKind::DateTime => "DATETIME",
            TextLiteralKind::Timestamp => "TIMESTAMP",
            TextLiteralKind::Json => "JSON",
        }
    }
}

/// One GoogleSQL literal, rendered so that it is a single primary expression whatever its
/// value holds: it can stand next to any operator without changing what the operator binds.
///
/// Only the constructors here build one, and every text they take is written through the
/// escaper, so holding a `SqlLiteral` means holding text that is safe to splice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlLiteral(String);

/// `text` as a single-quoted string literal.
fn quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('\'');
    escape_into(&mut out, text, '\'');
    out.push('\'');
    out
}

impl SqlLiteral {
    pub(crate) fn null() -> Self {
        Self("NULL".into())
    }

    pub(crate) fn string(value: &str) -> Self {
        Self(quoted(value))
    }

    /// A `b'...'` literal: printable ASCII as it is, every other byte as `\xhh`.
    pub(crate) fn bytes(value: &[u8]) -> Self {
        let mut out = String::with_capacity(value.len() + 3);
        out.push_str("b'");
        for &b in value {
            match b {
                b'\\' => out.push_str("\\\\"),
                b'\'' => out.push_str("\\'"),
                0x20..=0x7E => out.push(char::from(b)),
                _ => out.push_str(&format!("\\x{b:02x}")),
            }
        }
        out.push('\'');
        Self(out)
    }

    pub(crate) fn int64(value: i64) -> Self {
        if value == i64::MIN {
            // A literal is unary minus applied to an integer, and 9223372036854775808 is
            // above INT64.
            Self(format!("({} - 1)", i64::MIN + 1))
        } else {
            Self(value.to_string())
        }
    }

    /// A FLOAT64 literal, which always has a `.` or an exponent so it never reads as INT64.
    /// GoogleSQL has no literal for NaN or the infinities, so those are casts from a string.
    pub(crate) fn float64(value: f64) -> Self {
        if value.is_nan() {
            Self("CAST('nan' AS FLOAT64)".into())
        } else if value == f64::INFINITY {
            Self("CAST('inf' AS FLOAT64)".into())
        } else if value == f64::NEG_INFINITY {
            Self("CAST('-inf' AS FLOAT64)".into())
        } else {
            // `Debug` is the shortest text that parses back to the same `f64`, and it keeps a
            // `.0` on whole numbers.
            Self(format!("{value:?}"))
        }
    }

    pub(crate) fn bool(value: bool) -> Self {
        Self(if value { "TRUE" } else { "FALSE" }.into())
    }

    /// `KEYWORD '<text>'`. BigQuery parses the text as the kind; the crate's own encoders
    /// produce the canonical forms.
    pub(crate) fn text(kind: TextLiteralKind, text: &str) -> Self {
        Self(format!("{} {}", kind.keyword(), quoted(text)))
    }

    /// An INTERVAL in the canonical `Y-M D H:M:S[.F]` form.
    pub(crate) fn interval(text: &str) -> Self {
        Self(format!("INTERVAL {} YEAR TO SECOND", quoted(text)))
    }

    /// A RANGE from the canonical texts of its bounds, `None` for an unbounded end.
    pub(crate) fn range(
        element: BigQueryRangeElementType,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Self {
        let element = match element {
            BigQueryRangeElementType::Date => "DATE",
            BigQueryRangeElementType::DateTime => "DATETIME",
            BigQueryRangeElementType::Timestamp => "TIMESTAMP",
        };
        let text = format!(
            "[{}, {})",
            start.unwrap_or("UNBOUNDED"),
            end.unwrap_or("UNBOUNDED")
        );
        Self(format!("RANGE<{element}> {}", quoted(&text)))
    }

    pub(crate) fn array(items: Vec<SqlLiteral>) -> Self {
        let items: Vec<String> = items.into_iter().map(|i| i.0).collect();
        Self(format!("[{}]", items.join(", ")))
    }

    /// A `STRUCT(value AS name, ...)`, its field names quoted.
    pub(crate) fn struct_of(fields: Vec<(String, SqlLiteral)>) -> Self {
        let fields: Vec<String> = fields
            .into_iter()
            .map(|(name, value)| format!("{} AS {}", value.0, quote_identifier(&name)))
            .collect();
        Self(format!("STRUCT({})", fields.join(", ")))
    }

    #[cfg(test)]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for SqlLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The literal of a value in the query parameter form the type mapping encodes it to, so that
/// a literal and a parameter of one value always agree on its type. A scalar without a value
/// is NULL.
impl TryFrom<(&QueryParameterType, &QueryParameterValue)> for SqlLiteral {
    type Error = CodecError;

    fn try_from(
        (ty, value): (&QueryParameterType, &QueryParameterValue),
    ) -> Result<Self, Self::Error> {
        let malformed = |ty: &str, value: &dyn std::fmt::Debug| {
            CodecError::type_mismatch(format!("{value:?} is not a {ty} value"))
        };
        let kind = ty.r#type.as_str();
        let text = value.value.as_deref();
        let text_of = |kind: TextLiteralKind| text.map(|t| SqlLiteral::text(kind, t));
        let literal = match kind {
            "ARRAY" => {
                let element = ty
                    .array_type
                    .as_deref()
                    .ok_or_else(|| malformed("ARRAY", &"an ARRAY type with no element type"))?;
                let items = value
                    .array_values
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        SqlLiteral::try_from((element, item)).map_err(|e| e.at_index(i))
                    })
                    .collect::<Result<_, _>>()?;
                return Ok(SqlLiteral::array(items));
            }
            "STRUCT" => {
                let fields = ty
                    .struct_types
                    .iter()
                    .map(|field| {
                        let field_ty = field
                            .r#type
                            .as_ref()
                            .ok_or_else(|| malformed("STRUCT", &field.name))?;
                        let field_value = value
                            .struct_values
                            .get(&field.name)
                            .cloned()
                            .unwrap_or_default();
                        SqlLiteral::try_from((field_ty, &field_value))
                            .map(|v| (field.name.clone(), v))
                            .map_err(|e| e.at_field(&field.name))
                    })
                    .collect::<Result<_, _>>()?;
                return Ok(SqlLiteral::struct_of(fields));
            }
            "RANGE" => {
                let element = match ty.range_element_type.as_deref().map(|t| t.r#type.as_str()) {
                    Some("DATE") => BigQueryRangeElementType::Date,
                    Some("DATETIME") => BigQueryRangeElementType::DateTime,
                    Some("TIMESTAMP") => BigQueryRangeElementType::Timestamp,
                    other => return Err(malformed("RANGE element", &other)),
                };
                return Ok(match value.range_value.as_deref() {
                    None => SqlLiteral::null(),
                    Some(range) => {
                        let bound = |b: &Option<Box<QueryParameterValue>>| {
                            b.as_deref().and_then(|v| v.value.clone())
                        };
                        SqlLiteral::range(
                            element,
                            bound(&range.start).as_deref(),
                            bound(&range.end).as_deref(),
                        )
                    }
                });
            }
            "STRING" => text.map(SqlLiteral::string),
            "BYTES" => text
                .map(|t| {
                    bytes_from_base64(t)
                        .map(|b| SqlLiteral::bytes(&b))
                        .ok_or_else(|| malformed("BYTES", &t))
                })
                .transpose()?,
            "INT64" => text
                .map(|t| {
                    t.parse()
                        .map(SqlLiteral::int64)
                        .map_err(|_| malformed("INT64", &t))
                })
                .transpose()?,
            "FLOAT64" => text
                .map(|t| float_from_text(t).ok_or_else(|| malformed("FLOAT64", &t)))
                .transpose()?
                .map(SqlLiteral::float64),
            "BOOL" => text
                .map(|t| match t {
                    "true" => Ok(SqlLiteral::bool(true)),
                    "false" => Ok(SqlLiteral::bool(false)),
                    t => Err(malformed("BOOL", &t)),
                })
                .transpose()?,
            "NUMERIC" => text_of(TextLiteralKind::Numeric),
            "BIGNUMERIC" => text_of(TextLiteralKind::BigNumeric),
            "DATE" => text_of(TextLiteralKind::Date),
            "TIME" => text_of(TextLiteralKind::Time),
            "DATETIME" => text_of(TextLiteralKind::DateTime),
            "TIMESTAMP" => text_of(TextLiteralKind::Timestamp),
            "JSON" => text_of(TextLiteralKind::Json),
            "INTERVAL" => text.map(SqlLiteral::interval),
            other => {
                return Err(CodecError::new(
                    BigQueryCodecErrorKind::UnsupportedType,
                    format!("{other} has no literal form here"),
                ))
            }
        };
        Ok(literal.unwrap_or_else(SqlLiteral::null))
    }
}
