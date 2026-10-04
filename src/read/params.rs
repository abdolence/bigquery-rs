use crate::BigQueryInstant;
use crate::BigQueryTableRef;
use rsb_derive::Builder;

/// What a table read asks Storage Read for.
#[derive(Debug, PartialEq, Clone, Builder)]
pub struct BigQueryReadParams {
    /// The table to read.
    pub table: BigQueryTableRef,
    /// The columns to read, as given to `.fields(..)`; `None` lets a typed read project by
    /// itself, and an untyped read take every column.
    pub selected_fields: Option<Vec<String>>,
    /// A GoogleSQL filter such as `n > 10`, sent as the session's `row_restriction`.
    pub row_restriction: Option<String>,
    /// Reads the table as of this time instead of now.
    pub snapshot_time: Option<BigQueryInstant>,
    /// The percentage of the table to sample, from 0 to 100.
    pub sample_percentage: Option<f64>,
    /// How the session is opened.
    #[default = "BigQueryReadOptions::new()"]
    pub options: BigQueryReadOptions,
}

/// How a Storage Read session is opened.
#[derive(Debug, Eq, PartialEq, Clone, Builder)]
pub struct BigQueryReadOptions {
    /// The most read streams to ask for. Defaults to the machine's available parallelism.
    /// BigQuery decides the actual count and may return fewer.
    pub max_stream_count: Option<u32>,
    /// The fewest read streams BigQuery should aim for.
    pub preferred_min_stream_count: Option<u32>,
    /// The compression of the Arrow buffers on the wire.
    #[default = "BigQueryReadCompression::Lz4"]
    pub compression: BigQueryReadCompression,
}

/// The compression of the Arrow record batch buffers a read session sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigQueryReadCompression {
    /// Uncompressed buffers.
    None,
    /// LZ4 frame compression, the default.
    Lz4,
    /// Zstandard compression.
    Zstd,
}
