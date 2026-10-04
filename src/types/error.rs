use crate::errors::{
    BigQueryCodecErrorKind, BigQueryError, BigQueryErrorPublicGenericDetails,
    BigQuerySerializationError,
};
use std::fmt::{Display, Formatter};

/// One step of a field path, innermost first while the error travels outwards.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PathSegment {
    Field(String),
    Index(usize),
}

/// A codec failure on its way out of the read or write path.
///
/// The codecs raise it where the value fails and add the field path as the error travels out of
/// each nested struct and list, so the segments are kept innermost first and reversed only when
/// the error leaves the codec through [`CodecError::into_serialize`] or
/// [`CodecError::into_deserialize`], which also decide the direction.
#[derive(Clone, Debug)]
pub(crate) struct CodecError {
    kind: BigQueryCodecErrorKind,
    row: Option<u64>,
    reversed_path: Vec<PathSegment>,
    message: String,
}

impl CodecError {
    pub(crate) fn new(kind: BigQueryCodecErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            row: None,
            reversed_path: Vec::new(),
            message: message.into(),
        }
    }

    pub(crate) fn kind(&self) -> BigQueryCodecErrorKind {
        self.kind
    }

    /// Prefixes the path with a struct field, as the error leaves that field.
    pub(crate) fn at_field(mut self, name: &str) -> Self {
        self.reversed_path
            .push(PathSegment::Field(name.to_string()));
        self
    }

    /// Prefixes the path with a list position, as the error leaves that element.
    pub(crate) fn at_index(mut self, i: usize) -> Self {
        self.reversed_path.push(PathSegment::Index(i));
        self
    }

    pub(crate) fn with_row(mut self, row: u64) -> Self {
        self.row = Some(row);
        self
    }

    pub(crate) fn into_serialize(self) -> BigQueryError {
        BigQueryError::SerializeError(self.into_details())
    }

    pub(crate) fn into_deserialize(self) -> BigQueryError {
        BigQueryError::DeserializeError(self.into_details())
    }

    fn path(&self) -> String {
        let mut path = String::new();
        for segment in self.reversed_path.iter().rev() {
            match segment {
                PathSegment::Field(name) => {
                    if !path.is_empty() {
                        path.push('.');
                    }
                    path.push_str(name);
                }
                PathSegment::Index(i) => {
                    path.push('[');
                    path.push_str(&i.to_string());
                    path.push(']');
                }
            }
        }
        path
    }

    fn into_details(self) -> BigQuerySerializationError {
        let path = self.path();
        BigQuerySerializationError::new(
            BigQueryErrorPublicGenericDetails::new(self.kind.code().to_string()),
            self.kind,
            path,
            self.message,
        )
        .opt_row(self.row)
    }
}

impl Display for CodecError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;
        let path = self.path();
        if !path.is_empty() {
            write!(f, " at `{path}`")?;
        }
        write!(f, ": {}", self.message)
    }
}

impl std::error::Error for CodecError {}

impl serde::de::Error for CodecError {
    fn custom<T: Display>(msg: T) -> Self {
        Self::new(BigQueryCodecErrorKind::Custom, msg.to_string())
    }

    fn invalid_type(unexp: serde::de::Unexpected, exp: &dyn serde::de::Expected) -> Self {
        Self::new(
            BigQueryCodecErrorKind::TypeMismatch,
            format!("invalid type: {unexp}, expected {exp}"),
        )
    }

    /// A number that the target rejects is outside its range; any other rejected value is a
    /// text or form that does not parse.
    fn invalid_value(unexp: serde::de::Unexpected, exp: &dyn serde::de::Expected) -> Self {
        use serde::de::Unexpected;
        let kind = match unexp {
            Unexpected::Signed(_) | Unexpected::Unsigned(_) | Unexpected::Float(_) => {
                BigQueryCodecErrorKind::OutOfRange
            }
            _ => BigQueryCodecErrorKind::InvalidText,
        };
        Self::new(kind, format!("invalid value: {unexp}, expected {exp}"))
    }
}

impl serde::ser::Error for CodecError {
    fn custom<T: Display>(msg: T) -> Self {
        Self::new(BigQueryCodecErrorKind::Custom, msg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::{Error as _, Unexpected};

    fn details(err: CodecError) -> BigQuerySerializationError {
        match err.into_deserialize() {
            BigQueryError::DeserializeError(details) => details,
            other => panic!("expected a deserialize error, got {other:?}"),
        }
    }

    #[test]
    fn codec_error_kind_follows_serde_invalid_type_and_invalid_value() {
        let mismatch = details(CodecError::invalid_type(
            Unexpected::Str("x"),
            &"an integer",
        ));
        assert_eq!(mismatch.kind, BigQueryCodecErrorKind::TypeMismatch);
        assert_eq!(mismatch.public.code, "TYPE_MISMATCH");
        assert_eq!(
            mismatch.message,
            "invalid type: string \"x\", expected an integer"
        );

        for number in [
            Unexpected::Signed(-1),
            Unexpected::Unsigned(300),
            Unexpected::Float(1.5),
        ] {
            let err = details(CodecError::invalid_value(number, &"a u8"));
            assert_eq!(err.kind, BigQueryCodecErrorKind::OutOfRange, "{number}");
        }

        let text = details(CodecError::invalid_value(
            Unexpected::Str("2024-13-01"),
            &"a date",
        ));
        assert_eq!(text.kind, BigQueryCodecErrorKind::InvalidText);
        assert_eq!(
            text.message,
            "invalid value: string \"2024-13-01\", expected a date"
        );

        let custom = details(CodecError::missing_field("id"));
        assert_eq!(custom.kind, BigQueryCodecErrorKind::Custom);
        assert_eq!(custom.message, "missing field `id`");
    }

    #[test]
    fn path_reads_outermost_first_and_row_is_kept() {
        let err = CodecError::new(BigQueryCodecErrorKind::OutOfRange, "too big")
            .at_field("v")
            .at_index(1)
            .at_field("recs")
            .with_row(7);
        let err = details(err);
        assert_eq!(err.path, "recs[1].v");
        assert_eq!(err.row, Some(7));
    }

    #[test]
    fn serialize_direction_is_a_serialize_error() {
        let err = CodecError::new(BigQueryCodecErrorKind::NullForRequired, "null").into_serialize();
        assert!(matches!(err, BigQueryError::SerializeError(_)), "{err:?}");
    }
}
