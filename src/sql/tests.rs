//! Each test here protects one property a value or a name relies on to stay inside its token.
//!
//! [`pieces`] is a small GoogleSQL lexer for quoted tokens, written from the escape table on the
//! lexical page rather than from the renderer, so the two cannot share a mistake.

use super::*;
use crate::query::{infer_param, ParamLabel};
use crate::{
    BigQueryDate, BigQueryDateTime, BigQueryDecimal, BigQueryInterval, BigQueryJson, BigQueryRange,
    BigQueryRangeElementType, BigQueryTime, BigQueryTimestamp,
};
use serde::Serialize;

/// Values that would change a statement if they were spliced into its text. The live tests in
/// `tests/sql_live.rs` and the query parameter tests use the same set.
pub(crate) fn injection_corpus() -> Vec<String> {
    let mut corpus: Vec<String> = [
        "'; DROP TABLE x; --",
        "' OR '1'='1",
        "`backtick`",
        "\\'",
        "\\\\'",
        "\\\\",
        "\\",
        "\"",
        "\"\"\"",
        "'''",
        "/* comment */",
        "*/",
        "--",
        "#",
        "a\nb",
        "a\rb",
        "a\tb",
        "a\0b",
        "\u{2019} OR \u{2019}1\u{2019}=\u{2019}1",
        "\u{FF07}; DROP TABLE x; --",
        "\u{2028}\u{2029}\u{FEFF}\u{202E}",
        "@other_param",
        "?",
        "",
    ]
    .map(String::from)
    .into();
    corpus.push("'".repeat(1 << 20));
    corpus
}

/// Every character class the escaper has a rule for, beyond the corpus: all of C0 and C1, DEL,
/// the invisible format characters, a non-BMP character and the tag block.
pub(crate) fn every_char_class() -> String {
    let mut text: String = (0u32..0x250).filter_map(char::from_u32).collect();
    text.push_str(
        "\u{00AD}\u{061C}\u{200B}\u{200E}\u{2066}\u{2069}\u{FFF9}\u{1F600}\u{E0001}\u{E007F}",
    );
    text
}

/// One piece of a statement as GoogleSQL's lexer splits it, with every quoted token decoded.
#[derive(Debug, PartialEq)]
pub(crate) enum Piece {
    /// Text outside any quoted token.
    Text(String),
    String(String),
    Bytes(Vec<u8>),
    Identifier(String),
}

/// Splits `sql` into its quoted tokens, decoded, and the text between them. Panics on a token
/// that never closes, an unknown escape, or a raw newline inside a token.
pub(crate) fn pieces(sql: &str) -> Vec<Piece> {
    let mut out = Vec::new();
    let mut text = String::new();
    let mut chars = sql.chars();
    while let Some(quote) = chars.next() {
        if quote != '\'' && quote != '`' {
            text.push(quote);
            continue;
        }
        // The renderer quotes every name, so a bare `b` against a quote is a bytes prefix.
        let bytes = quote == '\'' && text.ends_with('b');
        if bytes {
            text.pop();
        }
        if !text.is_empty() {
            out.push(Piece::Text(std::mem::take(&mut text)));
        }
        let mut codes: Vec<u32> = Vec::new();
        loop {
            let character = chars
                .next()
                .unwrap_or_else(|| panic!("{quote} never closes in {sql:.80?}"));
            if character == quote {
                break;
            }
            assert!(
                character != '\n' && character != '\r',
                "a raw newline in a quoted token: {sql:.80?}"
            );
            assert!(
                !bytes || character.is_ascii(),
                "a bytes literal holds raw {character:?}"
            );
            codes.push(if character == '\\' {
                escape(&mut chars, bytes)
            } else {
                character.into()
            });
        }
        out.push(match (quote, bytes) {
            (_, true) => Piece::Bytes(
                codes
                    .into_iter()
                    .map(|code| u8::try_from(code).expect("a byte"))
                    .collect(),
            ),
            ('`', _) => Piece::Identifier(scalars(codes)),
            _ => Piece::String(scalars(codes)),
        });
    }
    if !text.is_empty() {
        out.push(Piece::Text(text));
    }
    out
}

/// The code an escape stands for, from the table on GoogleSQL's lexical page. A code that is
/// no character, or no byte in a bytes literal, fails when the token is assembled.
fn escape(chars: &mut std::str::Chars, bytes: bool) -> u32 {
    let escape = chars.next().expect("an escape after the backslash");
    let mut digits = |n: usize, radix: u32| {
        let digits: String = chars.by_ref().take(n).collect();
        assert_eq!(digits.len(), n, "{n} digits after \\{escape}");
        u32::from_str_radix(&digits, radix).expect("escape digits")
    };
    match escape {
        'a' => 0x07,
        'b' => 0x08,
        'f' => 0x0C,
        'n' => 0x0A,
        'r' => 0x0D,
        't' => 0x09,
        'v' => 0x0B,
        '\\' | '?' | '"' | '\'' | '`' => escape.into(),
        'x' | 'X' => digits(2, 16),
        'u' if !bytes => digits(4, 16),
        'U' if !bytes => digits(8, 16),
        '0'..='7' => escape.to_digit(8).expect("an octal digit") * 64 + digits(2, 8),
        other => panic!("\\{other} is not a GoogleSQL escape here"),
    }
}

fn scalars(codes: Vec<u32>) -> String {
    codes
        .into_iter()
        .map(|c| char::from_u32(c).expect("a scalar value"))
        .collect()
}

/// The value of `token`, which must be exactly one string literal.
pub(crate) fn lex_string(token: &str) -> String {
    match <[Piece; 1]>::try_from(pieces(token)) {
        Ok([Piece::String(s)]) => s,
        other => panic!("one STRING literal, got {other:.80?}"),
    }
}

fn infer_literal<V: Serialize>(value: V) -> SqlLiteral {
    let param = infer_param(ParamLabel::Positional(0), &value).expect("an inferable value");
    SqlLiteral::try_from((
        &param.parameter_type.expect("a type"),
        &param.parameter_value.expect("a value"),
    ))
    .expect("a literal")
}

#[test]
fn string_literal_is_one_token_that_decodes_to_the_value() {
    for value in injection_corpus().into_iter().chain([every_char_class()]) {
        let literal = SqlLiteral::string(&value);
        assert_eq!(lex_string(literal.as_str()), value, "{value:.80?}");
    }
}

#[test]
fn string_literal_writes_no_control_or_invisible_character_raw() {
    let literal = SqlLiteral::string(&every_char_class());
    let raw: Vec<char> = literal
        .as_str()
        .chars()
        .filter(|&c| c.is_control() || invisible(c))
        .collect();
    assert!(raw.is_empty(), "{raw:?} in {}", literal.as_str());
}

/// The format and separator characters the renderer writes as escapes, by the test's own
/// list.
fn invisible(c: char) -> bool {
    matches!(c, '\u{00AD}' | '\u{061C}' | '\u{200B}'..='\u{200F}' | '\u{2028}'..='\u{202E}'
        | '\u{2066}'..='\u{2069}' | '\u{FEFF}' | '\u{FFF9}'..='\u{FFFB}' | '\u{E0000}'..='\u{E007F}')
}

#[test]
fn string_literal_keeps_printable_text_readable() {
    assert_eq!(SqlLiteral::string("Åsa Öberg").as_str(), "'Åsa Öberg'");
    assert_eq!(SqlLiteral::string("it's").as_str(), "'it\\'s'");
    assert_eq!(SqlLiteral::string("a\\b").as_str(), "'a\\\\b'");
    assert_eq!(SqlLiteral::string("a\nb\0").as_str(), "'a\\nb\\x00'");
}

#[test]
fn bytes_literal_is_one_token_that_decodes_to_the_value() {
    let every_byte: Vec<u8> = (0..=255).collect();
    for value in injection_corpus()
        .into_iter()
        .map(String::into_bytes)
        .chain([every_byte])
    {
        let literal = SqlLiteral::bytes(&value);
        assert_eq!(
            pieces(literal.as_str()),
            [Piece::Bytes(value.clone())],
            "{value:.80?}"
        );
    }
}

#[test]
fn identifier_is_one_token_that_decodes_to_the_name() {
    for name in injection_corpus().into_iter().chain([every_char_class()]) {
        let quoted = quote_identifier(&name);
        assert_eq!(
            pieces(&quoted),
            [Piece::Identifier(name.clone())],
            "{name:.80?}"
        );
    }
}

#[test]
fn text_literals_are_a_keyword_and_one_escaped_string() {
    let kinds = [
        (TextLiteralKind::Numeric, "NUMERIC"),
        (TextLiteralKind::BigNumeric, "BIGNUMERIC"),
        (TextLiteralKind::Date, "DATE"),
        (TextLiteralKind::Time, "TIME"),
        (TextLiteralKind::DateTime, "DATETIME"),
        (TextLiteralKind::Timestamp, "TIMESTAMP"),
        (TextLiteralKind::Json, "JSON"),
    ];
    for (kind, name) in kinds {
        for text in injection_corpus().into_iter().take(24) {
            let literal = SqlLiteral::text(kind, &text);
            assert_eq!(
                pieces(literal.as_str()),
                [Piece::Text(format!("{name} ")), Piece::String(text.clone())],
                "{name} {text:?}"
            );
        }
    }
    let interval = SqlLiteral::interval("'; DROP TABLE x; --");
    assert_eq!(
        pieces(interval.as_str()),
        [
            Piece::Text("INTERVAL ".into()),
            Piece::String("'; DROP TABLE x; --".into()),
            Piece::Text(" YEAR TO SECOND".into()),
        ]
    );
}

#[test]
fn json_literal_carries_hostile_text_inside_the_document() {
    for value in injection_corpus() {
        #[derive(Serialize)]
        struct Doc<'a> {
            v: &'a str,
        }
        let literal = infer_literal(BigQueryJson(Doc { v: &value }));
        let decoded = pieces(literal.as_str());
        let [Piece::Text(keyword), Piece::String(text)] = &decoded[..] else {
            panic!("JSON and one string literal: {:.80}", literal.as_str());
        };
        assert_eq!(keyword, "JSON ");
        let doc: serde_json::Value = serde_json::from_str(text).expect("JSON text");
        assert_eq!(doc["v"], value.as_str(), "{value:.80?}");
    }
}

#[test]
fn scalar_literals_have_googlesql_forms() {
    assert_eq!(SqlLiteral::null().as_str(), "NULL");
    assert_eq!(SqlLiteral::bool(true).as_str(), "TRUE");
    assert_eq!(SqlLiteral::bool(false).as_str(), "FALSE");
    assert_eq!(SqlLiteral::int64(-42).as_str(), "-42");
    // `-9223372036854775808` lexes as negating an integer above INT64, so the minimum is
    // written as an expression.
    assert_eq!(
        SqlLiteral::int64(i64::MIN).as_str(),
        "(-9223372036854775807 - 1)"
    );
    assert_eq!(SqlLiteral::float64(1.5).as_str(), "1.5");
    assert_eq!(SqlLiteral::float64(1.0).as_str(), "1.0");
    assert_eq!(SqlLiteral::float64(-0.0).as_str(), "-0.0");
    assert_eq!(SqlLiteral::float64(1e300).as_str(), "1e300");
    assert_eq!(SqlLiteral::float64(5e-324).as_str(), "5e-324");
    assert_eq!(
        SqlLiteral::float64(f64::NAN).as_str(),
        "CAST('nan' AS FLOAT64)"
    );
    assert_eq!(
        SqlLiteral::float64(f64::INFINITY).as_str(),
        "CAST('inf' AS FLOAT64)"
    );
    assert_eq!(
        SqlLiteral::float64(f64::NEG_INFINITY).as_str(),
        "CAST('-inf' AS FLOAT64)"
    );
}

#[test]
fn values_render_with_the_type_their_serde_form_maps_to() {
    let ts: jiff::Timestamp = "2024-02-29T12:34:56.789012Z".parse().expect("valid");
    let date = jiff::civil::date(2024, 2, 29);
    let time = jiff::civil::time(12, 34, 56, 789_012_000);
    let cases: Vec<(SqlLiteral, &str)> = vec![
        (infer_literal("Åsa"), "'Åsa'"),
        (infer_literal(7_u8), "7"),
        (infer_literal(2.5_f32), "2.5"),
        (infer_literal(true), "TRUE"),
        (
            infer_literal(serde_bytes::Bytes::new(b"\x00'")),
            "b'\\x00\\''",
        ),
        (
            infer_literal(BigQueryTimestamp(ts)),
            "TIMESTAMP '2024-02-29 12:34:56.789012+00:00'",
        ),
        (infer_literal(BigQueryDate(date)), "DATE '2024-02-29'"),
        (infer_literal(BigQueryTime(time)), "TIME '12:34:56.789012'"),
        (
            infer_literal(BigQueryDateTime(date.to_datetime(time))),
            "DATETIME '2024-02-29 12:34:56.789012'",
        ),
        (
            infer_literal(BigQueryDecimal("123.45".to_string())),
            "NUMERIC '123.45'",
        ),
        (
            infer_literal(BigQueryDecimal(format!("1{}", "0".repeat(30)))),
            "BIGNUMERIC '1000000000000000000000000000000'",
        ),
        (
            infer_literal(BigQueryJson(serde_json::json!({"a": [1]}))),
            "JSON '{\"a\":[1]}'",
        ),
        (
            infer_literal(BigQueryInterval {
                months: 14,
                days: 3,
                nanos: 3_600_000_000_000,
            }),
            "INTERVAL '1-2 3 1:0:0' YEAR TO SECOND",
        ),
        (
            infer_literal(BigQueryRange {
                start: Some(BigQueryDate(date)),
                end: None,
            }),
            "RANGE<DATE> '[2024-02-29, UNBOUNDED)'",
        ),
        (infer_literal(["a", "b'"]), "['a', 'b\\'']"),
        (
            infer_literal(serde_json::json!({"n": 1, "s": "x"})),
            "STRUCT(1 AS `n`, 'x' AS `s`)",
        ),
        (
            infer_literal(std::collections::BTreeMap::from([("a`b", 1)])),
            "STRUCT(1 AS `a\\`b`)",
        ),
    ];
    for (literal, expected) in cases {
        assert_eq!(literal.as_str(), expected);
    }
    assert_eq!(
        SqlLiteral::range(BigQueryRangeElementType::Timestamp, None, None).as_str(),
        "RANGE<TIMESTAMP> '[UNBOUNDED, UNBOUNDED)'"
    );
}

#[test]
fn column_paths_quote_each_segment_whatever_it_holds() {
    for (path, sql) in [
        ("n", "`n`"),
        ("home.county", "`home`.`county`"),
        ("Åsa", "`Åsa`"),
        ("_PARTITIONTIME", "`_PARTITIONTIME`"),
        ("x OR TRUE", "`x OR TRUE`"),
        ("a`b", "`a\\`b`"),
        ("a\\b", "`a\\\\b`"),
        ("'; DROP TABLE x; --", "`'; DROP TABLE x; --`"),
        ("\u{202E}a", "`\\u202ea`"),
    ] {
        let parsed: ColumnPath = path.parse().expect(path);
        assert_eq!(parsed.sql(), sql, "{path}");
        assert_eq!(parsed.to_string(), path);
    }
    for name in injection_corpus()
        .into_iter()
        .filter(|n| !n.is_empty() && !n.contains('.') && !n.chars().any(char::is_control))
    {
        let parsed: ColumnPath = name.parse().expect("a name without control characters");
        assert_eq!(
            pieces(&parsed.sql()),
            [Piece::Identifier(name.clone())],
            "{name:.40?}"
        );
    }
}

#[test]
fn empty_segments_and_control_characters_are_refused() {
    for path in [
        "", ".", "a.", ".a", "a..b", "a\nb", "a\rb", "a\tb", "a\0b", "\u{7f}", "\u{85}x",
    ] {
        let err = path
            .parse::<ColumnPath>()
            .expect_err(&format!("{path:?} is refused"));
        assert_eq!(err.public.field, path, "the error names the path");
    }
}
