use crate::sql::{same_name, SqlParameterNames};
use std::borrow::Cow;

/// The GoogleSQL statement that [`query`](crate::BigQueryExprBuilder::query) runs: text built
/// at run time, or a `.sql` file embedded and checked by [`sql_file!`](crate::sql_file!).
///
/// Any `&str`, `String`, `Box<str>` or `Cow<str>` converts into it. A statement from
/// [`sql_file!`](crate::sql_file!) carries the names of its `@name` parameters: the query
/// refuses to run unless exactly those names are bound.
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
    /// Parameters are found as the [`sql_file!`](crate::sql_file!) docs describe, and names
    /// are compared ignoring ASCII case, as BigQuery compares them. The order of `parameters`
    /// and repeats in either list do not matter.
    ///
    /// In a `const` item a mismatch fails the build:
    ///
    /// ```compile_fail,E0080
    /// use bigquery::BigQuerySql;
    /// const TOP: BigQuerySql = BigQuerySql::from_static("SELECT @corpus", &["corpus", "limit"]);
    /// ```
    ///
    /// # Panics
    /// When called at run time, on a parameter of `text` that `parameters` does not list, or a
    /// name in `parameters` that `text` does not use.
    pub const fn from_static<const N: usize>(
        text: &'static str,
        parameters: &'static [&'static str; N],
    ) -> Self {
        let mut used = [false; N];
        let mut names = SqlParameterNames::new(text);
        while let Some(name) = names.next_name() {
            match position(parameters, name) {
                Some(index) => used[index] = true,
                None => mismatch(
                    "the SQL uses @",
                    name,
                    ", which the parameter list leaves out",
                ),
            }
        }
        let mut index = 0;
        while index < N {
            let name = parameters[index].as_bytes();
            if !matches!(position(parameters, name), Some(first) if used[first]) {
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
    pub(crate) fn into_parts(self) -> (String, Option<&'static [&'static str]>) {
        match self.0 {
            Statement::Text(text) => (text, None),
            Statement::Declared { text, parameters } => (text.to_string(), Some(parameters)),
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

impl From<Box<str>> for BigQuerySql {
    fn from(text: Box<str>) -> Self {
        Self(Statement::Text(text.into()))
    }
}

impl From<Cow<'_, str>> for BigQuerySql {
    fn from(text: Cow<'_, str>) -> Self {
        Self(Statement::Text(text.into_owned()))
    }
}

/// The index of the first of `parameters` that names the same parameter as `name`.
const fn position(parameters: &[&str], name: &[u8]) -> Option<usize> {
    let mut index = 0;
    while index < parameters.len() {
        let listed = parameters[index].as_bytes();
        // Most listed names differ in length; testing that here, without a call, keeps the
        // const evaluation of a long file within rustc's budget.
        if listed.len() == name.len() && same_name(listed, name) {
            return Some(index);
        }
        index += 1;
    }
    None
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
/// ```compile_fail,E0080
/// let _ = bigquery::sql_file!("sql/top_words.sql", corpus);
/// ```
///
/// So does a listed name the file never uses:
///
/// ```compile_fail,E0080
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
/// system variable. Whitespace and comments may stand between `@` and the name, and the name
/// may be backtick-quoted: ``@`corpus` `` lists as `corpus`. An unquoted name ends at the first
/// character that cannot be part of an identifier: `@window.earliest` reads the field
/// `earliest` of the STRUCT parameter `window`, and lists as `window`. Names are compared
/// ignoring ASCII case, as BigQuery compares them, so `@Corpus` lists as `corpus`.
#[macro_export]
macro_rules! sql_file {
    ($path:literal $(, $parameter:ident)* $(,)?) => {{
        #[allow(
            long_running_const_eval,
            reason = "the check is one pass over the file, which ends however long it is"
        )]
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
        assert_eq!(text, include_str!("sql/top_words.sql"));
        assert_eq!(parameters, Some(&["min_count", "corpus"][..]));
    }

    /// A `UNION ALL` branch of a sales report with no parameters.
    const REPORT_BRANCH: &str = "SELECT region, country, SUM(amount) AS total -- per region\n\
        FROM `sales.archived_orders` WHERE note != 'n/a' GROUP BY region, country\nUNION ALL\n";

    /// The report's last branch, which uses all of [`REPORT_FILTERS`].
    const REPORT_FILTERED_BRANCH: &str = "SELECT region, country, SUM(amount) AS total\n\
        FROM `sales.orders` WHERE ordered_on BETWEEN @start_date AND @end_date\n\
        AND region = @region AND country = @country AND city = @city AND store = @store\n\
        AND category = @category AND brand = @brand AND price BETWEEN @min_price AND @max_price\n\
        AND quantity BETWEEN @min_quantity AND @max_quantity AND currency = @currency\n\
        AND channel = @channel AND campaign = @campaign AND tier = @customer_tier\n\
        AND payment = @payment_method AND status = @status AND warehouse = @warehouse\n\
        AND carrier = @carrier GROUP BY region, country\n";

    const REPORT_FILTERS: [&str; 20] = [
        "start_date",
        "end_date",
        "region",
        "country",
        "city",
        "store",
        "category",
        "brand",
        "min_price",
        "max_price",
        "min_quantity",
        "max_quantity",
        "currency",
        "channel",
        "campaign",
        "customer_tier",
        "payment_method",
        "status",
        "warehouse",
        "carrier",
    ];

    const REPORT_BRANCHES: usize = 512;

    const REPORT_LENGTH: usize =
        REPORT_BRANCH.len() * REPORT_BRANCHES + REPORT_FILTERED_BRANCH.len();

    /// A report of [`REPORT_BRANCHES`] branches, its parameters only in the last one, so that
    /// finding them takes a scan of the whole text.
    static REPORT_BYTES: [u8; REPORT_LENGTH] = {
        let branch = REPORT_BRANCH.as_bytes();
        let filtered = REPORT_FILTERED_BRANCH.as_bytes();
        let padding = REPORT_LENGTH - filtered.len();
        let mut text = [0; REPORT_LENGTH];
        let mut index = 0;
        while index < REPORT_LENGTH {
            text[index] = if index < padding {
                branch[index % branch.len()]
            } else {
                filtered[index - padding]
            };
            index += 1;
        }
        text
    };

    const REPORT_TEXT: &str = match std::str::from_utf8(&REPORT_BYTES) {
        Ok(text) => text,
        Err(_) => panic!("the report is built from a UTF-8 branch"),
    };

    const REPORT: BigQuerySql = BigQuerySql::from_static(REPORT_TEXT, &REPORT_FILTERS);

    #[test]
    fn a_large_file_with_many_parameters_is_checked_while_compiling() {
        assert!(REPORT_TEXT.len() > 60_000);
        let (text, parameters) = REPORT.into_parts();
        assert_eq!(text, REPORT_TEXT);
        assert_eq!(parameters, Some(&REPORT_FILTERS[..]));
    }

    #[test]
    fn text_in_a_box_or_a_cow_is_a_statement() {
        let owned = String::from("SELECT 1");
        let statements = [
            BigQuerySql::from(Box::<str>::from("SELECT 1")),
            BigQuerySql::from(Cow::Borrowed("SELECT 1")),
            BigQuerySql::from(Cow::<str>::Owned(owned.clone())),
        ];
        for statement in statements {
            assert_eq!(statement, BigQuerySql::from(&owned));
        }
    }

    #[test]
    fn listed_names_match_parameters_whatever_their_case() {
        let _ = BigQuerySql::from_static("SELECT @Corpus WHERE c = @CORPUS", &["corpus"]);
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
