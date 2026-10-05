# Type mapping

The library reads and writes rows with serde, through its own codecs:

- reads, from Storage Read and from inline query results, go through one Arrow decoder;
- writes, through Storage Write, go through one protobuf encoder.

Both codecs share one mapping, so every BigQuery type has one set of Rust forms, the same in both
directions. Whatever you write in a form, you can read back in the same form.

The library re-exports `jiff`, so the temporal types need no extra dependency.

## Modes

A column's mode decides the Rust shape around its type:

| | NULLABLE | REQUIRED | REPEATED |
|---|---|---|---|
| Rust field | `Option<T>` | `T` | `Vec<T>` |
| also on read | a bare `T`, which fails a NULL row with `NullForNonOption` | `Option<T>`, always `Some` | `Option<Vec<T>>`, always `Some` |
| `None` on write | stored as NULL | `NullForRequired` | an empty array |
| field missing from the Rust row on write | stored as NULL | `MissingRequiredField` | an empty array |
| NULL element on write | | | `NullArrayElement` |

BigQuery never stores a NULL array: an empty REPEATED column reads as an empty `Vec`. A REPEATED
column can be any serde sequence on both sides, such as `VecDeque<T>`, `BTreeSet<T>` or `[T; N]`
when the array has exactly N elements.

An ARRAY column is the REPEATED mode of its element type, so there is no ARRAY type of its own.
BigQuery has no array of arrays as a column type, and the encoder refuses one with
`UnsupportedType`.

## Every type at a glance

"Default" works with plain `#[derive(Serialize, Deserialize)]` and no attributes. The other forms
work in both directions too.

| BigQuery type | Default Rust type | Other forms | Wrapper |
|---|---|---|---|
| INT64 | `i64` | other integer types, range-checked | |
| FLOAT64 | `f64` | `f32` | |
| NUMERIC, BIGNUMERIC | `String` | integers, `f64` | `BigQueryDecimal<T>` |
| BOOL | `bool` | | |
| STRING | `String` | `Box<str>`, `char`, unit enums, any type serialized as a string | |
| BYTES | `Vec<u8>` | `serde_bytes::ByteBuf`, `[u8; N]` | |
| DATE | `jiff::civil::Date` | `String`, `i32` days | `BigQueryDate` |
| TIME | `jiff::civil::Time` | `String`, `i64` microseconds of the day | `BigQueryTime` |
| DATETIME | `jiff::civil::DateTime` | `String`, `i64` civil microseconds | `BigQueryDateTime` |
| TIMESTAMP | `jiff::Timestamp` | `String`, `i64` microseconds since the epoch | `BigQueryTimestamp` |
| GEOGRAPHY | `String` (WKT) | | |
| JSON | by the Rust type, see [JSON](#json) | | `BigQueryJson<T>` |
| INTERVAL | `BigQueryInterval` | `String` | |
| RANGE | `BigQueryRange<T>` | | |
| STRUCT | a struct with derived serde | `HashMap<String, V>`, `BTreeMap<String, V>`, `serde_json::Value` | |

A form not in this table is not part of the mapping, even if it happens to work. A form the column
does not take fails that row with `TypeMismatch`, in either direction.

A typical row looks like this:

```rust
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: i64,
    customer: Option<String>,
    total: String,
    paid: bool,
    tags: Vec<String>,
    placed_on: jiff::civil::Date,
    placed_at: Option<jiff::Timestamp>,
    shipping: Option<Address>,
    attributes: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct Address {
    city: String,
    street: Option<String>,
}
```

## Integers and floats

INT64 reads into any Rust integer type, `i8` to `i128` and `u8` to `u128`, and a value that does not
fit fails that row with `OutOfRange`. On write a value above `i64::MAX` is `OutOfRange`.

FLOAT64 is `f64`, or `f32`, which is cast on read with no range check. NaN, both infinities and
`-0.0` round-trip exactly. Integers are not taken for FLOAT64 and floats are not taken for INT64,
in either direction.

## NUMERIC and BIGNUMERIC

NUMERIC holds 38 digits with 9 after the point, BIGNUMERIC 76 digits with 38 after the point. A
`NUMERIC(P, S)` or `BIGNUMERIC(P, S)` column narrows them further.

- **`String`** is the default and loses nothing. On read it is the canonical text with trailing
  fractional zeros removed, so `0.00` comes back as `"0"`. On write it is a plain decimal with an
  optional sign and no exponent.
- **Integers** are exact both ways. A value with a fractional part read into an integer fails that
  row with `OutOfRange`.
- **`f64`** works both ways and is lossy, rounded to 9 fractional digits (38 for BIGNUMERIC) on
  write and parsed into the nearest `f64` on read.
- **`BigQueryDecimal<T>`** holds any decimal type that round-trips through its `Display` and
  `FromStr`, such as `bigdecimal::BigDecimal` or `rust_decimal::Decimal`. The text form is used
  whatever the type's own serde does, since some decimal types serialize as `f64`. For a bare field
  there are `serialize_as_decimal` and `serialize_as_optional_decimal`.

```rust
# mod bigdecimal { pub type BigDecimal = String; }
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Invoice {
    total: BigQueryDecimal<bigdecimal::BigDecimal>,
    #[serde(with = "bigquery::serialize_as_optional_decimal")]
    discount: Option<bigdecimal::BigDecimal>,
}
```

On write, a NUMERIC with more than 29 digits before the point or more than 9 after it, a
BIGNUMERIC outside its range or with more than 38 digits after the point, NaN and the infinities
fail that row with `OutOfRange`. A text that `T::from_str`
rejects on read, for example `rust_decimal::Decimal` above its 28 digits, fails with `Custom`.

Be aware of what BigQuery does on a `NUMERIC(P, S)` column. The library sends every value at the
full scale of the type, and BigQuery rounds the digits beyond the column's scale half away from
zero, with no error. So `1.245` and `-1.245` written to a `NUMERIC(10, 2)` column are stored as
`1.25` and `-1.25`. A value over the column's precision is checked by BigQuery only.

## BOOL, STRING and BYTES

BOOL is `bool` only. Integers and strings are not taken for it.

STRING is `String`, `Box<str>`, a `char` for one character, a unit-variant enum, or any type whose
serde form is a string, such as `uuid::Uuid`. The maximum length of a `STRING(n)` column is checked
by BigQuery only.

BYTES is `Vec<u8>`, `serde_bytes::ByteBuf`, or `[u8; N]` when the value has exactly N bytes. A string
is not taken for BYTES on write, and bytes are not taken for STRING: BigQuery stores a string sent
to BYTES as its raw UTF-8 bytes and does not decode base64, so a base64 text would be stored as
text by accident. A `serde_json::Value` row cannot hold BYTES; select `TO_BASE64(b)` instead, or use
a typed field.

## GEOGRAPHY

GEOGRAPHY is a `String`. BigQuery returns WKT and takes both WKT and GeoJSON on write, storing
either as a geography. Invalid text is rejected by BigQuery.

## Dates and times

The four temporal types map to jiff by default, with no attribute:

| BigQuery type | jiff type | Range | `String` form | Integer form |
|---|---|---|---|---|
| DATE | `jiff::civil::Date` | 0001-01-01 to 9999-12-31 | `YYYY-MM-DD` | `i32` days since 1970-01-01 |
| TIME | `jiff::civil::Time` | 00:00:00 to 23:59:59.999999 | `HH:MM:SS[.ffffff]` | `i64` microseconds of the day |
| DATETIME | `jiff::civil::DateTime` | 0001-01-01T00:00:00 to 9999-12-31T23:59:59.999999 | `YYYY-MM-DDTHH:MM:SS[.ffffff]` | `i64` civil microseconds since 1970-01-01T00:00:00 |
| TIMESTAMP | `jiff::Timestamp` | 0001-01-01 to 9999-12-31T23:59:59.999999 UTC | RFC 3339 | `i64` microseconds since the epoch |

Some details for each:

- BigQuery keeps microseconds, so sub-microsecond digits are dropped on write, floored for
  TIMESTAMP;
- a DATE or DATETIME before year 1 on write is `OutOfRange`, since jiff allows years down to -9999;
- a DATETIME has no offset, so `jiff::Timestamp` is not taken for it, and a TIMESTAMP is not taken
  by `jiff::civil::DateTime` or `jiff::Zoned`;
- a DATETIME `String` uses `T` on read and takes `T` or a space on write;
- a TIMESTAMP `String` is always `...Z` on read, and on write needs `Z` or `±HH:MM`, with `T` or a
  space between date and time. Other shapes, such as `+00` or ` UTC`, fail with `InvalidText`.

Be aware of the TIMESTAMP range. `jiff::Timestamp` ends at `9999-12-30T22:00:00.999999999Z`, a day
before BigQuery's maximum. A TIMESTAMP above it read into a jiff type, the wrapper included, fails
that row with `OutOfRange`. `String` and `i64` hold the full range, so read into one of those if
your data can have such values, for example `9999-12-31` used as "never". The same holds for the
TIMESTAMP ends of a `RANGE<TIMESTAMP>`.

TIMESTAMP columns with picosecond precision (`timestamp_precision = 12`) are not supported yet, and
fail with `UnsupportedType`.

### Temporal wrappers

jiff's serde speaks text only, so a plain jiff field is printed as text by the codec and parsed by
jiff, on both read and write. The wrappers keep the jiff types and skip the text:

- `BigQueryTimestamp(pub jiff::Timestamp)`;
- `BigQueryDate(pub jiff::civil::Date)`;
- `BigQueryTime(pub jiff::civil::Time)`;
- `BigQueryDateTime(pub jiff::civil::DateTime)`.

With the library's codecs they read and write BigQuery's integers directly. In any other serde
format they are the same as the plain jiff type, so a row of wrappers serializes to the same JSON as
a row of plain jiff fields. For a field you do not want to change the type of, the `with` modules do
the same:

```rust
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    happened_at: BigQueryTimestamp,
    day: Option<BigQueryDate>,
    #[serde(with = "bigquery::serialize_as_timestamp")]
    received_at: jiff::Timestamp,
    #[serde(with = "bigquery::serialize_as_optional_datetime")]
    local_time: Option<jiff::civil::DateTime>,
}
```

The modules are `serialize_as_{timestamp,date,time,datetime}` and their
`serialize_as_optional_..` forms. A wrapper is checked against its column, so a `BigQueryDate` on a
TIMESTAMP column fails with `TypeMismatch`, since its integer form would read microseconds as days.

Use them for wide tables or hot paths. `benches/read_codec.rs` and `benches/write_codec.rs` compare
plain jiff fields, the wrappers and integers.

## JSON

A JSON column maps by the Rust type, with no wrapper:

- a `String` field is the JSON text as it is, in both directions;
- any other serde shape, such as `serde_json::Value`, a typed struct, a map or a sequence, is parsed
  on read and printed as JSON on write.

The same holds for a JSON field inside a STRUCT and for the elements of an `ARRAY<JSON>`:

```rust
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    kind: String,
    amount: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    // JSON column, as text
    raw: String,
    // JSON column, as any JSON value
    document: serde_json::Value,
    // JSON column, parsed into a struct
    payload: Option<Payload>,
    // ARRAY<JSON> column
    events: Vec<serde_json::Value>,
}
```

JSON has two kinds of null, SQL NULL and the JSON value `null`, and the rules for them are:

- `Option<serde_json::Value>` keeps them apart: SQL NULL is `None`, JSON `null` is
  `Some(Value::Null)`;
- `Option<String>` gets SQL NULL as `None` and JSON `null` as the text `"null"`;
- any other `Option<T>`, such as `Option<Payload>` or `Option<i64>`, reads both as `None`;
- a bare typed field, such as `Payload`, fails a JSON `null` with `Custom`.

Text that does not parse into the Rust type on read fails that row with `Custom`. On write, invalid
JSON text from a `String` field is checked by BigQuery only: the append fails with `RowErrors`, with
a `FIELDS_ERROR` for that row, and no row of that batch is stored.

`BigQueryJson<T>` is for the two cases the rule above leaves out:

- `BigQueryJson<String>` is a JSON string value parsed into a `String`, where a plain `String`
  field would be the raw text with its quotes;
- a top-level `Value::String` is written the way a `String` is, as the JSON text itself. A
  document that can be a bare JSON string needs `BigQueryJson<serde_json::Value>`.

In other serde formats `BigQueryJson<T>` is the JSON text as a string. For a bare field there are
`serialize_as_json` and `serialize_as_optional_json`.

## INTERVAL

An INTERVAL has three parts, each with its own sign, so `-1 month +3 days` is a valid value. That is
what `BigQueryInterval` holds:

```rust
use bigquery::*;

let interval = BigQueryInterval {
    months: -1,
    days: 3,
    nanos: 4 * 3_600 * 1_000_000_000,
};

// jiff::Span has one sign for all its units, so the conversion can fail
let span: Result<jiff::Span, _> = interval.try_into();
assert!(span.is_err());

let interval = BigQueryInterval::try_from(jiff::Span::new().days(3).hours(4))?;
assert_eq!(interval.days, 3);
# Ok::<(), bigquery::errors::BigQueryError>(())
```

A `String` works too, in BigQuery's canonical form `[-]Y-M [-]D [-]H:M:S[.ffffff]`, for example
`1-2 -3 4:5:6.000789`.

BigQuery keeps microseconds, so `nanos` that are not whole microseconds fail the row with
`OutOfRange` on write. Be aware of one BigQuery limit: Storage Read fails the whole stream for an
INTERVAL whose time part is beyond about 2,562,047 hours, and no Arrow client can work around it. So
the library refuses such a value on write, where it could not be read back. A value like that
written another way can still be read with `CAST(iv AS STRING)` in a query.

## RANGE

`BigQueryRange<T>` holds a `RANGE<DATE>`, `RANGE<DATETIME>` or `RANGE<TIMESTAMP>`, with `T` any
Rust form of the element type, the temporal wrappers included:

```rust
use bigquery::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Promotion {
    valid: Option<BigQueryRange<jiff::civil::Date>>,
    window: BigQueryRange<BigQueryTimestamp>,
}
```

`start` is inclusive and `end` exclusive, and `None` is an unbounded end. A NULL range into a bare
`BigQueryRange<T>` fails with `NullForNonOption`. Whether `start` comes before `end` is checked by
BigQuery.

Be aware of a REQUIRED RANGE column: BigQuery reads an unbounded end of it as `1970-01-01` (or the
epoch for DATETIME and TIMESTAMP), and the library cannot tell it from a real epoch bound. NULLABLE
and REPEATED RANGE columns keep unbounded ends as `None`.

## STRUCT

A STRUCT column is a struct with derived serde, its fields matched by name, or a map with string
keys, or a `serde_json::Value` object. Nested structs and `ARRAY<STRUCT>` work the same way.

- a NULL STRUCT into a bare struct fails with `NullForNonOption`, also when every field of the
  struct is an `Option`, so use `Option<Address>` for a NULLABLE STRUCT;
- on write a Rust field with no column fails with `UnknownField` naming the path, for example
  `shipping.inner.zip`, and a REQUIRED subfield the row never wrote with `MissingRequiredField`;
- a tuple works as a whole row only, a STRUCT column into a tuple is `TypeMismatch`.

Full example available [here](https://github.com/abdolence/bigquery-rs/blob/master/examples/nested-structs-and-json.rs).

## Field names and serde attributes

Fields are matched to columns by name, so the order of the fields does not matter. The usual serde
attributes work in both directions:

- `rename` and `rename_all`, for example for camelCase columns;
- `alias`, for a column that has another name in some tables;
- `skip`, `default`, `flatten` and `deny_unknown_fields`;
- a missing `Option` column reads as `None`, and a missing column with `default` takes its default.

When an aliased field finds both its own column and the alias column in one table, the row fails
with serde's `duplicate field`, the same as `serde_json` gives for that input. A hand-written
`Deserialize` that answers struct fields by its own integer numbering is not supported; one that
takes field names works.

`path!` builds column names from Rust fields. For a field renamed to camelCase use
`path_camel_case!`, for any other `#[serde(rename)]` the column name as a string.

## Dynamic rows

A row can be a `serde_json::Value`, a map with string keys, or an `#[serde(untagged)]` enum on read,
when the shape is not known up front. For whole Arrow batches with no serde at all, the read and
query builders have `record_batches()`.

## Query parameters

Query parameters take the same Rust forms. `.param(name, value)` infers the type from the value's
serde form:

- integers are INT64, floats FLOAT64, strings STRING, `bool` BOOL, `serde_bytes` values BYTES;
- sequences are ARRAY, structs and string-keyed maps STRUCT;
- the wrappers are their own types: `BigQueryTimestamp` is TIMESTAMP, `BigQueryDate` DATE, etc.,
  `BigQueryJson<T>` JSON and `BigQueryInterval` INTERVAL;
- `BigQueryDecimal<T>` is NUMERIC, or BIGNUMERIC for a value NUMERIC cannot hold;
- `BigQueryRange<T>` is RANGE when its bounds are temporal wrappers.

A `Vec<u8>` serializes as a sequence of integers, so it is an `ARRAY<INT64>`; use
`serde_bytes::ByteBuf` or `param_as` for BYTES.

A plain jiff value serializes as text, so it is inferred as a STRING. `None`, an empty sequence and
a sequence of mixed types cannot be inferred either. For those, `.param_as(name, type, value)` sets
the type and takes the value in any form the write path takes for it, and `None` there is a NULL of
that type:

```rust,no_run
# use bigquery::*;
# async fn example(db: BigQueryDb, since: jiff::Timestamp) -> BigQueryResult<()> {
let outcome = db
    .fluent()
    .query(
        "SELECT COUNT(*) AS n FROM shop.orders \
         WHERE placed_at >= @since AND (@customer IS NULL OR customer = @customer)",
    )
    .param_as("since", BigQueryFieldType::Timestamp, since)
    .param_as(
        "customer",
        BigQueryFieldType::String { max_length: None },
        None::<String>,
    )
    .execute()
    .await?;
# let _ = outcome;
# Ok(())
# }
```

`BigQueryParamType::array_of(..)` declares an ARRAY parameter.

## Schema types

Table schemas use one vocabulary, whatever API they came from: `BigQueryTableSchema` with its
`BigQueryFieldSchema` columns, each with a name, a `BigQueryFieldType`, a `BigQueryFieldMode`, a
description and a default value expression.

The v2 API names types with the legacy names, the Storage API with an enum, and Arrow with physical
types only, so the library normalises all of them. `INTEGER` is `Int64`, `FLOAT` is `Float64`,
`BOOLEAN` is `Bool`, `RECORD` is `Struct`, `DECIMAL` is `Numeric`, `BIGDECIMAL` is `BigNumeric`, and
an empty mode, as DDL creates it, is `Nullable`. Type parameters are part of the type:
`String { max_length }`, `Numeric(Some(BigQueryDecimalParams { precision, scale }))`, etc. So two
schemas compare with `==`.

`Display` prints GoogleSQL type syntax, such as `STRING(10)`, `NUMERIC(10, 2)`, `RANGE<DATE>` and
`STRUCT<a INT64, b ARRAY<STRING>>`.

## Codec errors

A value that does not fit fails as `BigQueryError::SerializeError` on write and
`BigQueryError::DeserializeError` on read. Both carry a `BigQuerySerializationError` with a
`BigQueryCodecErrorKind`, the field path, such as `recs[1].v`, and on read the row index. A read
error fails only its own row: the other rows of the batch still decode.

| Kind | When |
|---|---|
| `TypeMismatch` | the Rust form is not one the column's type takes, or a temporal wrapper is on a column of another type |
| `NullForNonOption` | read: NULL into a target that is not an `Option`, a NULL STRUCT or RANGE included |
| `NullForRequired` | write: `None` for a REQUIRED field |
| `NullArrayElement` | write: `None` inside a REPEATED field |
| `OutOfRange` | the value is outside the BigQuery type (a year before 1, NUMERIC digits, the INTERVAL time part) or outside the Rust target (a narrower integer, jiff's TIMESTAMP maximum) |
| `InvalidText` | a text form that does not parse, such as a malformed DATE, TIMESTAMP, NUMERIC or INTERVAL |
| `UnknownField` | write: a field or map key with no column |
| `MissingRequiredField` | write: a REQUIRED field the row never wrote |
| `UnsupportedType` | a column type the library does not handle, such as TIMESTAMP with picosecond precision |
| `RowTooLarge` | write: one encoded row is larger than the request budget |
| `Custom` | anything the target type's own serde impl raises, such as `missing field` or a `FromStr` failure |
