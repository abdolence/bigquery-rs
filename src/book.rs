//! Compiles the code in the book chapters as doctests, so an example cannot go stale without a
//! test failing. Only reachable through `cfg(doctest)`, which rustdoc sets while collecting
//! doctests and never while building the crate.

#[doc = include_str!("../docs/src/intro.md")]
mod intro {}

/// The README's quick start, compiled so it cannot drift from the API it demonstrates.
#[doc = include_str!("../README.md")]
mod readme {}
