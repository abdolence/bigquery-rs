//! GoogleSQL text that carries values: escaped literals and backtick-quoted identifiers.
//!
//! Every place the crate writes a value or a name into SQL text goes through this module, so
//! that a value can never end the token it is in. The lexical rules come from GoogleSQL's
//! "Lexical structure and syntax" page,
//! <https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical>: the escape table
//! of "Escape sequences for string and bytes literals", the quoting rules of "Quoted
//! identifiers", and the literal forms of the sections that follow.

mod ident;
mod literal;
mod parameters;

pub(crate) use ident::*;
pub(crate) use literal::*;
pub(crate) use parameters::*;

#[cfg(test)]
pub(crate) mod tests;
