//! Query parameters: each `QueryParameter` becomes a `ScalarValue` of exactly the Arrow type its
//! `parameter_type` names, an ARRAY a `List`, a STRUCT a `Struct`, a NULL a typed null, with the
//! wire metadata of JSON, GEOGRAPHY and RANGE, so no type is ever inferred from context.
//!
//! Values arrive as the typed text the client writes: INT64 decimal text; FLOAT64 shortest
//! round-trip text, `NaN`, `Infinity`, `-Infinity`; BOOL `true` or `false`; BYTES base64; DATE
//! `YYYY-MM-DD`; TIME `HH:MM:SS[.ffffff]`; DATETIME `YYYY-MM-DD HH:MM:SS[.ffffff]`; TIMESTAMP the
//! same with `+00:00`; NUMERIC canonical decimal text; INTERVAL `[-]Y-M [-]D [-]H:M:S[.ffffff]`;
//! RANGE, ARRAY and STRUCT as their nested values.

use super::ParameterStyle;
use crate::error::EngineError;
use crate::types::{EXTENSION_NAME, GEOGRAPHY_EXTENSION, JSON_EXTENSION, RANGE_SQL_TYPE, SQL_TYPE};
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalMonthDayNano, IntervalUnit, TimeUnit,
};
use base64::Engine as _;
use datafusion::common::metadata::{FieldMetadata, ScalarAndMetadata};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use gcloud_sdk::google::cloud::bigquery::v2::{
    QueryParameter, QueryParameterType, QueryParameterValue,
};
use std::collections::HashMap;
use std::sync::Arc;

/// The zone label TIMESTAMP parameters are bound with; it meets the stored `+00:00` label
/// through DataFusion's own type coercion.
const UTC: &str = "UTC";

/// A request's query parameters as typed scalars, by name and in order.
#[derive(Debug, Clone, Default)]
pub(crate) struct Parameters {
    named: HashMap<String, ScalarAndMetadata>,
    positional: Vec<ScalarAndMetadata>,
}

/// The parameters of a request: every one with a name is named, every one without is positional.
///
/// # Errors
/// [`EngineError::Invalid`] naming a parameter whose type or value is not one BigQuery sends,
/// and [`EngineError::Unsupported`] for a BIGNUMERIC parameter.
impl TryFrom<&[QueryParameter]> for Parameters {
    type Error = EngineError;

    fn try_from(parameters: &[QueryParameter]) -> Result<Self, Self::Error> {
        let mut bound = Parameters::default();
        for (index, parameter) in parameters.iter().enumerate() {
            let what = if parameter.name.is_empty() {
                format!("query parameter {}", index + 1)
            } else {
                format!("query parameter @{}", parameter.name)
            };
            let parameter_type = parameter
                .parameter_type
                .as_ref()
                .ok_or_else(|| EngineError::invalid(&what, "the parameter has no type"))?;
            let field = ParameterField::of(&what, "", parameter_type)?;
            let value = field.scalar(&what, parameter.parameter_value.as_ref())?;
            let metadata = (!field.field.metadata().is_empty())
                .then(|| FieldMetadata::from(field.field.metadata()));
            let scalar = ScalarAndMetadata::new(value, metadata);
            if parameter.name.is_empty() {
                bound.positional.push(scalar);
            } else {
                bound.named.insert(parameter.name.clone(), scalar);
            }
        }
        Ok(bound)
    }
}

impl Parameters {
    /// `plan` with every placeholder of `style` replaced by its parameter.
    ///
    /// # Errors
    /// DataFusion's planning error, naming the placeholder, when one has no parameter.
    pub(crate) fn bind(
        &self,
        plan: LogicalPlan,
        style: ParameterStyle,
    ) -> Result<LogicalPlan, DataFusionError> {
        match style {
            ParameterStyle::None => Ok(plan),
            ParameterStyle::Named => plan.with_param_values(ParamValues::Map(self.named.clone())),
            ParameterStyle::Positional => {
                plan.with_param_values(ParamValues::List(self.positional.clone()))
            }
        }
    }
}

/// The Arrow field a parameter type names, and how its values read.
struct ParameterField {
    field: Field,
    kind: ParameterKind,
}

/// The shape of a parameter type's values.
enum ParameterKind {
    /// A value read from its text.
    Text(TextForm),
    Array(Box<ParameterField>),
    Struct(Vec<(String, ParameterField)>),
    Range(Box<ParameterField>),
}

/// How a scalar parameter's text reads.
#[derive(Clone, Copy)]
enum TextForm {
    Int64,
    Float64,
    Bool,
    String,
    Bytes,
    Interval,
    /// Read by Arrow's own cast from text: NUMERIC, DATE, TIME, DATETIME and TIMESTAMP.
    Cast,
}

impl ParameterField {
    /// The field of a parameter, or of a field or element of one, named `name`.
    fn of(
        what: &str,
        name: &str,
        parameter_type: &QueryParameterType,
    ) -> Result<Self, EngineError> {
        let metadata =
            |key: &str, value: &str| HashMap::from([(key.to_string(), value.to_string())]);
        let (data_type, metadata, kind) = match parameter_type.r#type.to_ascii_uppercase().as_str()
        {
            "INT64" | "INTEGER" => (DataType::Int64, HashMap::new(), TextForm::Int64.into()),
            "FLOAT64" | "FLOAT" => (DataType::Float64, HashMap::new(), TextForm::Float64.into()),
            "BOOL" | "BOOLEAN" => (DataType::Boolean, HashMap::new(), TextForm::Bool.into()),
            "STRING" => (DataType::Utf8, HashMap::new(), TextForm::String.into()),
            "JSON" => (
                DataType::Utf8,
                metadata(EXTENSION_NAME, JSON_EXTENSION),
                TextForm::String.into(),
            ),
            "GEOGRAPHY" => (
                DataType::Utf8,
                metadata(EXTENSION_NAME, GEOGRAPHY_EXTENSION),
                TextForm::String.into(),
            ),
            "BYTES" => (DataType::Binary, HashMap::new(), TextForm::Bytes.into()),
            "NUMERIC" | "DECIMAL" => (
                DataType::Decimal128(38, 9),
                HashMap::new(),
                TextForm::Cast.into(),
            ),
            "DATE" => (DataType::Date32, HashMap::new(), TextForm::Cast.into()),
            "TIME" => (
                DataType::Time64(TimeUnit::Microsecond),
                HashMap::new(),
                TextForm::Cast.into(),
            ),
            "DATETIME" => (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                HashMap::new(),
                TextForm::Cast.into(),
            ),
            "TIMESTAMP" => (
                DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
                HashMap::new(),
                TextForm::Cast.into(),
            ),
            "INTERVAL" => (
                DataType::Interval(IntervalUnit::MonthDayNano),
                HashMap::new(),
                TextForm::Interval.into(),
            ),
            "BIGNUMERIC" | "BIGDECIMAL" => {
                return Err(EngineError::unsupported(
                    "BIGNUMERIC",
                    format!("the type of {what}"),
                ))
            }
            "ARRAY" => {
                let element_type = parameter_type.array_type.as_deref().ok_or_else(|| {
                    EngineError::invalid(what, "an ARRAY parameter names its element type")
                })?;
                let element = ParameterField::of(what, "item", element_type)?;
                // The element's own metadata names the list on the wire.
                let metadata = element.field.metadata().clone();
                let item = Arc::new(element.field.clone().with_metadata(HashMap::new()));
                (
                    DataType::List(item),
                    metadata,
                    ParameterKind::Array(Box::new(element)),
                )
            }
            "STRUCT" | "RECORD" => {
                let fields = parameter_type
                    .struct_types
                    .iter()
                    .map(|field| {
                        let field_type = field.r#type.as_ref().ok_or_else(|| {
                            EngineError::invalid(
                                what,
                                format!("STRUCT field `{}` has no type", field.name),
                            )
                        })?;
                        Ok((
                            field.name.clone(),
                            ParameterField::of(what, &field.name, field_type)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, EngineError>>()?;
                let arrow_fields: Fields = fields
                    .iter()
                    .map(|(_, field)| Arc::new(field.field.clone()))
                    .collect::<Vec<FieldRef>>()
                    .into();
                (
                    DataType::Struct(arrow_fields),
                    HashMap::new(),
                    ParameterKind::Struct(fields),
                )
            }
            "RANGE" => {
                let element_type =
                    parameter_type
                        .range_element_type
                        .as_deref()
                        .ok_or_else(|| {
                            EngineError::invalid(what, "a RANGE parameter names its element type")
                        })?;
                let bound = ParameterField::of(what, "start", element_type)?;
                let bounds = Fields::from(vec![
                    bound.field.clone(),
                    bound.field.clone().with_name("end"),
                ]);
                (
                    DataType::Struct(bounds),
                    metadata(SQL_TYPE, RANGE_SQL_TYPE),
                    ParameterKind::Range(Box::new(bound)),
                )
            }
            other => {
                return Err(EngineError::invalid(
                    what,
                    format!("{other:?} is not a parameter type"),
                ))
            }
        };
        Ok(Self {
            field: Field::new(name, data_type, true).with_metadata(metadata),
            kind,
        })
    }

    /// The scalar `value` holds, a typed NULL when it holds nothing.
    fn scalar(
        &self,
        what: &str,
        value: Option<&QueryParameterValue>,
    ) -> Result<ScalarValue, EngineError> {
        let invalid = |detail: String| EngineError::invalid(what, detail);
        let data_type = self.field.data_type();
        let null = || ScalarValue::try_from(data_type).map_err(|err| invalid(err.to_string()));
        match &self.kind {
            ParameterKind::Text(form) => {
                let Some(text) = value.and_then(|value| value.value.as_ref()) else {
                    return null();
                };
                form.scalar(text, data_type).map_err(invalid)
            }
            ParameterKind::Array(element) => {
                let elements = value
                    .map(|value| value.array_values.as_slice())
                    .unwrap_or_default()
                    .iter()
                    .map(|element_value| element.scalar(what, Some(element_value)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ScalarValue::List(ScalarValue::new_list_nullable(
                    &elements,
                    element.field.data_type(),
                )))
            }
            ParameterKind::Struct(fields) => {
                let Some(values) = value
                    .map(|value| &value.struct_values)
                    .filter(|values| !values.is_empty())
                else {
                    return Ok(ScalarStructBuilder::new_null(
                        fields
                            .iter()
                            .map(|(_, field)| field.field.clone())
                            .collect::<Vec<Field>>(),
                    ));
                };
                let mut builder = ScalarStructBuilder::new();
                for (name, field) in fields {
                    let scalar = field.scalar(what, values.get(name))?;
                    builder = builder.with_scalar(Arc::new(field.field.clone()), scalar);
                }
                builder.build().map_err(|err| invalid(err.to_string()))
            }
            ParameterKind::Range(bound) => {
                let Some(range) = value.and_then(|value| value.range_value.as_deref()) else {
                    return null();
                };
                let DataType::Struct(bounds) = data_type else {
                    return null();
                };
                let start = bound.scalar(what, range.start.as_deref())?;
                let end = bound.scalar(what, range.end.as_deref())?;
                let mut builder = ScalarStructBuilder::new();
                for (field, scalar) in bounds.iter().zip([start, end]) {
                    builder = builder.with_scalar(field.clone(), scalar);
                }
                builder.build().map_err(|err| invalid(err.to_string()))
            }
        }
    }
}

impl From<TextForm> for ParameterKind {
    fn from(form: TextForm) -> Self {
        ParameterKind::Text(form)
    }
}

impl TextForm {
    /// The scalar of `data_type` that `text` holds.
    fn scalar(self, text: &str, data_type: &DataType) -> Result<ScalarValue, String> {
        let unreadable = |rule: &str| format!("{text:?} is not {rule}");
        Ok(match self {
            TextForm::Int64 => {
                ScalarValue::Int64(Some(text.parse().map_err(|_| unreadable("an INT64"))?))
            }
            TextForm::Float64 => {
                ScalarValue::Float64(Some(text.parse().map_err(|_| unreadable("a FLOAT64"))?))
            }
            TextForm::Bool => {
                ScalarValue::Boolean(Some(match text.to_ascii_lowercase().as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(unreadable("a BOOL")),
                }))
            }
            TextForm::String => ScalarValue::Utf8(Some(text.to_string())),
            TextForm::Bytes => ScalarValue::Binary(Some(
                base64::engine::general_purpose::STANDARD
                    .decode(text)
                    .map_err(|_| unreadable("base64 BYTES"))?,
            )),
            TextForm::Interval => ScalarValue::IntervalMonthDayNano(Some(
                parse_interval(text).ok_or_else(|| unreadable("an INTERVAL"))?,
            )),
            TextForm::Cast => ScalarValue::try_from_string(text.to_string(), data_type)
                .map_err(|err| format!("{text:?} is not a {data_type} value: {err}"))?,
        })
    }
}

/// An INTERVAL in BigQuery's canonical text, `[-]Y-M [-]D [-]H:M:S[.ffffff]`.
fn parse_interval(text: &str) -> Option<IntervalMonthDayNano> {
    let mut parts = text.split_whitespace();
    let (year_month, day, time) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let (negative_months, year_month) = strip_sign(year_month);
    let (years, months) = year_month.split_once('-')?;
    let months = years
        .parse::<i32>()
        .ok()?
        .checked_mul(12)?
        .checked_add(months.parse().ok()?)?;
    let days = day.parse::<i32>().ok()?;
    let (negative_time, time) = strip_sign(time);
    let mut fields = time.splitn(3, ':');
    let (hours, minutes, seconds) = (fields.next()?, fields.next()?, fields.next()?);
    let (whole, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
    if fraction.len() > 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let fraction_nanos = format!("{fraction:0<9}").parse::<i64>().ok()?;
    let nanos = hours
        .parse::<i64>()
        .ok()?
        .checked_mul(3600)?
        .checked_add(minutes.parse::<i64>().ok()?.checked_mul(60)?)?
        .checked_add(whole.parse::<i64>().ok()?)?
        .checked_mul(1_000_000_000)?
        .checked_add(fraction_nanos)?;
    Some(IntervalMonthDayNano::new(
        if negative_months { -months } else { months },
        days,
        if negative_time { -nanos } else { nanos },
    ))
}

/// The text without its leading `-`, and whether it had one.
fn strip_sign(text: &str) -> (bool, &str) {
    match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_text_reads_as_months_days_and_nanoseconds() {
        assert_eq!(
            parse_interval("1-2 3 4:5:6.789"),
            Some(IntervalMonthDayNano::new(
                14,
                3,
                ((4 * 3600 + 5 * 60 + 6) * 1_000_000_000) + 789_000_000
            ))
        );
        assert_eq!(
            parse_interval("-1-2 -3 -0:0:1"),
            Some(IntervalMonthDayNano::new(-14, -3, -1_000_000_000))
        );
        assert_eq!(parse_interval("1-2 3"), None);
    }

    #[test]
    fn a_json_array_parameter_names_its_elements_on_the_list() {
        let parameter_type = QueryParameterType {
            r#type: "ARRAY".to_string(),
            array_type: Some(Box::new(QueryParameterType {
                r#type: "JSON".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let field = ParameterField::of("@docs", "docs", &parameter_type).expect("a valid type");
        assert_eq!(
            field.field.metadata(),
            &HashMap::from([(EXTENSION_NAME.to_string(), JSON_EXTENSION.to_string())])
        );
    }
}
