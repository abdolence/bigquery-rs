//! The proto3 field conventions of the v2 and Storage APIs, in both directions: a time or a
//! duration is an integer count of milliseconds.

use crate::errors::BigQueryError;
use crate::types::error::CodecError;
use crate::{BigQueryInstant, BigQueryResult};
use std::time::Duration;

/// `duration` in whole milliseconds as the integer type of the request field `field`.
///
/// # Errors
/// [`BigQueryError::InvalidParametersError`] for `field` if the milliseconds do not fit `T`.
pub(crate) fn millis<T: TryFrom<u128>>(field: &str, duration: Duration) -> BigQueryResult<T> {
    T::try_from(duration.as_millis()).map_err(|_| {
        BigQueryError::invalid_parameters(
            field,
            format!(
                "{duration:?} is {} ms, which does not fit in {}",
                duration.as_millis(),
                std::any::type_name::<T>()
            ),
        )
    })
}

/// A v2 timestamp in milliseconds since the epoch; 0 is unset.
///
/// # Errors
/// [`BigQueryError::DeserializeError`] of kind `OutOfRange` at `field` for a value outside the
/// timestamp range.
pub(crate) fn timestamp_ms(field: &str, ms: i64) -> BigQueryResult<Option<BigQueryInstant>> {
    if ms == 0 {
        return Ok(None);
    }
    BigQueryInstant::from_millisecond(ms)
        .map(Some)
        .map_err(|err| {
            CodecError::out_of_range(format!("{ms} ms is not a timestamp: {err}"))
                .at_field(field)
                .into_deserialize()
        })
}

/// A v2 duration in milliseconds; unset or 0 is no duration.
///
/// # Errors
/// [`BigQueryError::DeserializeError`] of kind `OutOfRange` at `field` for a negative value.
pub(crate) fn duration_ms(field: &str, ms: Option<i64>) -> BigQueryResult<Option<Duration>> {
    match ms.filter(|ms| *ms != 0) {
        None => Ok(None),
        Some(ms) => u64::try_from(ms)
            .map(|ms| Some(Duration::from_millis(ms)))
            .map_err(|_| {
                CodecError::out_of_range(format!("{ms} ms is a negative duration"))
                    .at_field(field)
                    .into_deserialize()
            }),
    }
}
