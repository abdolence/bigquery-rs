use crate::sql::{same_bytes, SqlParameterNames};
use std::borrow::Cow;

/// The GoogleSQL statement that [`query`](crate::BigQueryExprBuilder::query) runs: text built
/// at run time, or a `.sql` file embedded and checked by [`sql_file!`](crate::sql_file!).
///
/// Any `&str` or `String` converts into it; a `&str` is copied. A statement from
/// [`sql_file!`](crate::sql_file!) borrows the text embedded in the binary, and it carries the
/// names of its `@name` parameters: the query refuses to run unless exactly those names are
/// bound.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigQuerySql(Statement);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Statement {
    Text(String),
    Declared {
        text: &'static str,
        parameters: &'static [&'static str],
    },
}

impl BigQuerySql {
    /// Wraps `text`, whose `@name` parameters must be exactly `parameters`, without allocating.
    /// This is what [`sql_file!`](crate::sql_file!) expands to.
    ///
    /// Parameters are compared by exact name; they are found as the
    /// [`sql_file!`](crate::sql_file!) docs describe. The order of `parameters` and repeats in
    /// either list do not matter.
    ///
    /// In a `const` item a mismatch fails the build:
    ///
    /// ```compile_fail
    /// use bigquery::BigQuerySql;
    /// const TOP: BigQuerySql = BigQuerySql::from_static("SELECT @corpus", &["corpus", "limit"]);
    /// ```
    ///
    /// # Panics
    /// When called at run time, on a parameter of `text` that `parameters` does not list, or a
    /// name in `parameters` that `text` does not use.
    pub const fn from_static(text: &'static str, parameters: &'static [&'static str]) -> Self {
        let mut used = SqlParameterNames::new(text);
        while let Some(name) = used.next_name() {
            if !lists(parameters, name) {
                mismatch(
                    "the SQL uses @",
                    name,
                    ", which the parameter list leaves out",
                );
            }
        }
        let mut index = 0;
        while index < parameters.len() {
            let name = parameters[index].as_bytes();
            if !SqlParameterNames::new(text).contains(name) {
                mismatch(
                    "the parameter list names ",
                    name,
                    ", which the SQL never uses",
                );
            }
            index += 1;
        }
        Self(Statement::Declared { text, parameters })
    }

    /// The text and, for a statement from [`from_static`](Self::from_static), its parameter
    /// names.
    pub(crate) fn into_parts(self) -> (Cow<'static, str>, Option<&'static [&'static str]>) {
        match self.0 {
            Statement::Text(text) => (Cow::Owned(text), None),
            Statement::Declared { text, parameters } => (Cow::Borrowed(text), Some(parameters)),
        }
    }
}

impl From<String> for BigQuerySql {
    fn from(text: String) -> Self {
        Self(Statement::Text(text))
    }
}

impl From<&str> for BigQuerySql {
    fn from(text: &str) -> Self {
        Self(Statement::Text(text.to_string()))
    }
}

impl From<&String> for BigQuerySql {
    fn from(text: &String) -> Self {
        Self(Statement::Text(text.clone()))
    }
}

const fn lists(parameters: &[&str], name: &[u8]) -> bool {
    let mut index = 0;
    while index < parameters.len() {
        if same_bytes(parameters[index].as_bytes(), name) {
            return true;
        }
        index += 1;
    }
    false
}

/// Panics with `before`, `name` and `after` joined. Const panics take a single `&str` argument,
/// so the message is assembled in a buffer; a name too long for it is cut.
const fn mismatch(before: &str, name: &[u8], after: &str) -> ! {
    let mut message = [0u8; 256];
    let mut length = 0;
    let parts = [before.as_bytes(), name, after.as_bytes()];
    let mut part = 0;
    while part < parts.len() {
        let mut index = 0;
        while index < parts[part].len() && length < message.len() {
            message[length] = parts[part][index];
            length += 1;
            index += 1;
        }
        part += 1;
    }
    match std::str::from_utf8(message.split_at(length).0) {
        Ok(message) => panic!("{}", message),
        Err(_) => panic!("{}", before),
    }
}

/// Embeds a GoogleSQL file as a [`BigQuerySql`] and checks, as the calling crate compiles, that
/// the names listed after the path are exactly the file's `@name` parameters.
///
/// The path is relative to the file that calls the macro, as for [`include_str!`], and Cargo
/// rebuilds the crate when the `.sql` file changes. Pass the result to
/// [`query`](crate::BigQueryExprBuilder::query) and bind each value with `.param`, as for a
/// query written inline:
///
/// ```rust,no_run
/// use bigquery::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct WordCount {
///     word: String,
///     word_count: i64,
/// }
///
/// # async fn example(db: BigQueryDb) -> BigQueryResult<()> {
/// let top_words: Vec<WordCount> = db
///     .fluent()
///     .query(bigquery::sql_file!("sql/top_words.sql", corpus, min_count))
///     .param("corpus", "hamlet")
///     .param("min_count", 100)
///     .obj()
///     .query()
///     .await?;
/// # let _ = top_words;
/// # Ok(())
/// # }
/// ```
///
/// A parameter the file uses but the list leaves out fails the build:
///
/// ```compile_fail
/// let _ = bigquery::sql_file!("sql/top_words.sql", corpus);
/// ```
///
/// So does a listed name the file never uses:
///
/// ```compile_fail
/// let _ = bigquery::sql_file!("sql/top_words.sql", corpus, min_count, limit);
/// ```
///
/// The compiler cannot see which names the `.param` calls bind, so the query checks them when
/// it runs: a terminal fails with
/// [`InvalidParametersError`](crate::errors::BigQueryError::InvalidParametersError), naming the
/// parameter, before any request is sent if a listed name is not bound or a bound name is not
/// listed.
///
/// The file is read with GoogleSQL's lexical rules, so an `@` inside a string or bytes literal,
/// a backtick-quoted identifier or a comment is not a parameter, and neither is a `@@name`
/// system variable. A parameter name ends at the first character that cannot be part of an
/// identifier: `@window.earliest` reads the field `earliest` of the STRUCT parameter `window`,
/// and lists as `window`. Names are compared exactly, case included.
#[macro_export]
macro_rules! sql_file {
    ($path:literal $(, $parameter:ident)* $(,)?) => {{
        const SQL: $crate::BigQuerySql = $crate::BigQuerySql::from_static(
            ::core::include_str!($path),
            &[$(::core::stringify!($parameter)),*],
        );
        SQL
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_with_its_parameters_listed_is_embedded_unchanged() {
        let (text, parameters) =
            crate::sql_file!("sql/top_words.sql", min_count, corpus,).into_parts();
        assert!(matches!(text, Cow::Borrowed(_)));
        assert_eq!(text, include_str!("sql/top_words.sql"));
        assert_eq!(parameters, Some(&["min_count", "corpus"][..]));
    }

    #[test]
    #[should_panic]
    fn an_unlisted_parameter_panics_at_run_time() {
        let text = "SELECT @corpus, @min_count";
        let _ = BigQuerySql::from_static(text, &["corpus"]);
    }

    #[test]
    #[should_panic]
    fn a_listed_name_the_text_never_uses_panics_at_run_time() {
        let _ = BigQuerySql::from_static("SELECT '@limit' -- @limit", &["limit"]);
    }
}
