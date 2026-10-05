use crate::errors::{BigQueryInvalidParametersError, BigQueryInvalidParametersPublicDetails};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// `name` as a backtick-quoted identifier. Any name is accepted: backticks, backslashes, the
/// other quotes and every character that is not printable are written as escapes, so the
/// identifier ends at its closing backtick whatever the name holds.
pub(crate) fn quote_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('`');
    escape_into(&mut out, name, '`');
    out.push('`');
    out
}

/// Writes `text` as the body of a token quoted with `quote`: a single-quoted string literal or
/// a backtick-quoted identifier, which share one escape table.
///
/// Only `quote` and the backslash need escaping to keep the token closed; the other quotes
/// are left as they are, so JSON text stays readable inside a string literal. Control
/// characters and the invisible format and separator characters are written as `\x`, `\u` or
/// `\U` escapes, so that the SQL text shows exactly what the value holds; everything else,
/// look-alike quotes included, is a plain character to the lexer and is written as it is.
pub(crate) fn escape_into(out: &mut String, text: &str, quote: char) {
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() || is_invisible(c) => {
                let code = u32::from(c);
                // In a string literal `\xhh` is the character U+00hh, so it is only used for
                // ASCII, where it reads the same as in a bytes literal.
                if code < 0x80 {
                    out.push_str(&format!("\\x{code:02x}"));
                } else if code <= 0xFFFF {
                    out.push_str(&format!("\\u{code:04x}"));
                } else {
                    out.push_str(&format!("\\U{code:08x}"));
                }
            }
            c => out.push(c),
        }
    }
}

/// Format and separator characters that render as nothing or reorder the text around them:
/// the soft hyphen, the bidirectional marks and overrides, zero-width characters, the line
/// and paragraph separators, the byte order mark, interlinear annotations and the tag block.
fn is_invisible(c: char) -> bool {
    matches!(c,
        '\u{00AD}'
        | '\u{061C}'
        | '\u{180E}'
        | '\u{200B}'..='\u{200F}'
        | '\u{2028}'..='\u{202E}'
        | '\u{2060}'..='\u{206F}'
        | '\u{FEFF}'
        | '\u{FFF9}'..='\u{FFFB}'
        | '\u{E0000}'..='\u{E007F}')
}

/// A column, or a field inside STRUCT columns, as dotted segments, none empty and none
/// holding a control character.
///
/// The quoting in [`sql`](Self::sql) is what keeps any name a name, so the crate checks only
/// what it relies on itself and leaves BigQuery's column name rules to BigQuery, which fails
/// the read on a name a table cannot have.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColumnPath {
    segments: Vec<String>,
}

impl ColumnPath {
    /// The path as SQL, each segment a quoted identifier.
    pub(crate) fn sql(&self) -> String {
        self.segments
            .iter()
            .map(|s| quote_identifier(s))
            .collect::<Vec<_>>()
            .join(".")
    }
}

/// Why `segment` cannot be one segment of a column path, if it cannot: it is empty or holds a
/// control character, which cannot be quoted into SQL unambiguously.
pub(crate) fn column_segment_violation(segment: &str) -> Option<String> {
    if segment.is_empty() {
        return Some("is empty".into());
    }
    segment
        .chars()
        .find(|character| character.is_control())
        .map(|character| format!("holds the control character {character:?}"))
}

/// `name` under the dotted path `prefix`, or `name` alone when `prefix` is the top level.
pub(crate) fn dotted_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

impl FromStr for ColumnPath {
    type Err = BigQueryInvalidParametersError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        let segments: Vec<String> = path.split('.').map(String::from).collect();
        if let Some(violation) = segments
            .iter()
            .find_map(|segment| column_segment_violation(segment))
        {
            return Err(BigQueryInvalidParametersError::new(
                BigQueryInvalidParametersPublicDetails::new(
                    path.to_string(),
                    format!("the column path {path:?} has a segment that {violation}"),
                ),
            ));
        }
        Ok(Self { segments })
    }
}

impl Display for ColumnPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.segments.join("."))
    }
}
