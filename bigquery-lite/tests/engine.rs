//! The engine through the `bigquery` client and gcloud-sdk's generated clients, as an
//! application reaches it.

mod engine {
    pub(crate) mod support;

    mod datasets_and_tables;
    mod in_process;
    mod listener;
}
