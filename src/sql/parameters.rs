//! The named parameters of a GoogleSQL statement, found in const context so that
//! [`sql_file!`](crate::sql_file!) can compare them with the names it lists while the calling
//! crate compiles.

/// The `@name` query parameters of a statement, in order of appearance, repeats included.
///
/// A parameter is `@` followed by an identifier: an ASCII letter or `_`, then ASCII letters,
/// digits and `_`. The name ends at the first other character, so `@window.earliest` is the
/// parameter `window` and its field `earliest`. Not parameters: `@@name` system variables,
/// `@{...}` hints, and any `@` inside a string or bytes literal (`'...'`, `"..."`, `'''...'''`,
/// `"""..."""`, with or without an `r`, `b` or `rb` prefix), a backtick-quoted identifier, or a
/// `--`, `#` or `/* */` comment.
///
/// A backslash skips the character after it inside every quoted token. That is the escape rule
/// of the non-raw forms, and in a raw literal a backslash still keeps the quote after it from
/// ending the literal, so the prefix never changes where a token ends. An unterminated token or
/// comment runs to the end of the text.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SqlParameterNames<'a> {
    rest: &'a [u8],
}

impl<'a> SqlParameterNames<'a> {
    pub(crate) const fn new(sql: &'a str) -> Self {
        Self {
            rest: sql.as_bytes(),
        }
    }

    /// The next parameter name, or `None` past the last one. The name is ASCII.
    pub(crate) const fn next_name(&mut self) -> Option<&'a [u8]> {
        let bytes = self.rest;
        let mut at = 0;
        while at < bytes.len() {
            let next = byte_at(bytes, at + 1);
            at = match bytes[at] {
                b'\'' | b'"' | b'`' => quoted_end(bytes, at),
                b'-' if next == b'-' => line_end(bytes, at),
                b'#' => line_end(bytes, at),
                b'/' if next == b'*' => block_comment_end(bytes, at),
                b'@' if next == b'@' => identifier_end(bytes, at + 2),
                b'@' if is_identifier_start(next) => {
                    let end = identifier_end(bytes, at + 1);
                    let (head, tail) = bytes.split_at(end);
                    self.rest = tail;
                    return Some(head.split_at(at + 1).1);
                }
                _ => at + 1,
            };
        }
        self.rest = &[];
        None
    }

    /// Whether `name` is one of the remaining parameters.
    pub(crate) const fn contains(mut self, name: &[u8]) -> bool {
        while let Some(found) = self.next_name() {
            if same_bytes(found, name) {
                return true;
            }
        }
        false
    }
}

/// Byte equality; `==` on slices is not `const`.
pub(crate) const fn same_bytes(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// The byte at `at`, or `0` past the end, which no rule here matches.
const fn byte_at(bytes: &[u8], at: usize) -> u8 {
    if at < bytes.len() {
        bytes[at]
    } else {
        0
    }
}

const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

const fn identifier_end(bytes: &[u8], mut at: usize) -> usize {
    while at < bytes.len() && (bytes[at].is_ascii_alphanumeric() || bytes[at] == b'_') {
        at += 1;
    }
    at
}

/// The end of the quoted token that opens at `start`: one quote character, or three of `'` or
/// `"`.
const fn quoted_end(bytes: &[u8], start: usize) -> usize {
    let quote = bytes[start];
    let triple =
        quote != b'`' && byte_at(bytes, start + 1) == quote && byte_at(bytes, start + 2) == quote;
    let mut at = if triple { start + 3 } else { start + 1 };
    while at < bytes.len() {
        if bytes[at] == b'\\' {
            at += 2;
        } else if bytes[at] != quote {
            at += 1;
        } else if !triple {
            return at + 1;
        } else if byte_at(bytes, at + 1) == quote && byte_at(bytes, at + 2) == quote {
            return at + 3;
        } else {
            at += 1;
        }
    }
    bytes.len()
}

/// The end of the line comment that opens at `start`, at its line break.
const fn line_end(bytes: &[u8], mut at: usize) -> usize {
    while at < bytes.len() && bytes[at] != b'\n' && bytes[at] != b'\r' {
        at += 1;
    }
    at
}

/// The end of the `/* */` comment that opens at `start`. GoogleSQL block comments do not nest.
const fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut at = start + 2;
    while at + 1 < bytes.len() {
        if bytes[at] == b'*' && bytes[at + 1] == b'/' {
            return at + 2;
        }
        at += 1;
    }
    bytes.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn names(sql: &str) -> Vec<String> {
        let mut scan = SqlParameterNames::new(sql);
        let mut found = Vec::new();
        while let Some(name) = scan.next_name() {
            found.push(String::from_utf8_lossy(name).into_owned());
        }
        found
    }

    #[test]
    fn named_parameters_are_found_in_order_with_repeats() {
        assert_eq!(
            names(
                "SELECT word FROM t WHERE corpus = @corpus AND word_count >= @min_count \
                 OR corpus = @corpus"
            ),
            ["corpus", "min_count", "corpus"]
        );
    }

    #[test]
    fn a_name_ends_at_the_first_non_identifier_character() {
        assert_eq!(
            names("WHERE y BETWEEN @years.earliest AND @years.latest AND x IN UNNEST(@ids)"),
            ["years", "years", "ids"]
        );
        assert_eq!(names("@_a1,@b2+@c"), ["_a1", "b2", "c"]);
    }

    #[test]
    fn system_variables_hints_and_bare_at_signs_are_not_parameters() {
        assert_eq!(
            names("SET @@dataset_id = 'x'; SELECT @@project_id, @1, @ a, @{hint=1} @real"),
            ["real"]
        );
    }

    #[test]
    fn quoted_tokens_hide_their_contents() {
        let sql = r#"SELECT 'a@x', "b@x", '''c@x ' '' @x''', """d@x " @x""", r'e@x', b"f@x",
                     rb'''g@x''', `h@x`, 'i\'@x', "j\"@x", `k\`@x`, r'l\'@x', @found"#;
        assert_eq!(names(sql), ["found"]);
    }

    #[test]
    fn comments_hide_their_contents() {
        let sql = "SELECT 1 -- @a\n, 2 # @b\r\n, /* @c\n @d */ @found /*/ @e */ - @minus";
        assert_eq!(names(sql), ["found", "minus"]);
    }

    #[test]
    fn unterminated_tokens_run_to_the_end() {
        assert_eq!(names("SELECT @a, 'x @b"), ["a"]);
        assert_eq!(names("SELECT @a /* @b"), ["a"]);
        assert_eq!(names("SELECT @a, '''x ' @b"), ["a"]);
        assert_eq!(names("SELECT @a, '\\"), ["a"]);
    }

    #[test]
    fn contains_looks_only_past_the_names_already_taken() {
        let mut scan = SqlParameterNames::new("@first @second");
        assert!(scan.contains(b"first"));
        assert_eq!(scan.next_name(), Some(&b"first"[..]));
        assert!(!scan.contains(b"first"));
        assert!(scan.contains(b"second"));
        assert!(!scan.contains(b"secon"));
    }

    /// One piece of a generated statement: either text that holds no `@` and no token opener,
    /// a parameter, or a token that hides a parameter-looking body.
    #[derive(Clone, Debug)]
    enum Piece {
        Plain(String),
        Parameter(String),
        Hidden(String),
    }

    fn identifier() -> impl Strategy<Value = String> {
        "[A-Za-z_][A-Za-z0-9_]{0,8}"
    }

    fn piece() -> impl Strategy<Value = Piece> {
        let hidden_body = (identifier(), "[a-z ]{0,4}")
            .prop_map(|(name, filler)| format!("{filler}@{name} {filler}"));
        prop_oneof![
            "[ ,()=<>+*.;0-9\n]{1,6}".prop_map(Piece::Plain),
            identifier().prop_map(Piece::Parameter),
            (hidden_body, 0usize..9).prop_map(|(body, form)| Piece::Hidden(match form {
                0 => format!("'{body}'"),
                1 => format!("\"{body}\""),
                2 => format!("'''{body}'''"),
                3 => format!("\"\"\"{body}\"\"\""),
                4 => format!("r'{body}'"),
                5 => format!("`{body}`"),
                6 => format!("/* {body} */"),
                7 => format!("-- {body}\n"),
                _ => format!("# {body}\n"),
            })),
        ]
    }

    proptest! {
        #[test]
        fn exactly_the_parameters_outside_tokens_are_found(
            pieces in proptest::collection::vec(piece(), 0..12)
        ) {
            let mut sql = String::new();
            let mut expected = Vec::new();
            for piece in &pieces {
                // A space keeps a piece from extending the identifier or `@` before it.
                sql.push(' ');
                match piece {
                    Piece::Plain(text) | Piece::Hidden(text) => sql.push_str(text),
                    Piece::Parameter(name) => {
                        sql.push('@');
                        sql.push_str(name);
                        expected.push(name.clone());
                    }
                }
            }
            prop_assert_eq!(names(&sql), expected);
        }

        #[test]
        fn arbitrary_text_never_panics_and_yields_identifiers(sql in "\\PC{0,64}") {
            for name in names(&sql) {
                prop_assert!(is_identifier_start(name.as_bytes()[0]));
                prop_assert_eq!(identifier_end(name.as_bytes(), 0), name.len());
            }
        }
    }
}
