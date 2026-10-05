# Reading data

The library reads rows in two ways, and decodes both with the same Arrow decoder, into your
structures with serde or as raw Arrow record batches:

- [Reading tables](./reading-tables.md): a table read through the Storage Read API, with column
  projection, typed filters and parallel streams. It runs no query and no job;
- [Queries](./queries.md): GoogleSQL with parameters, through the v2 `Query` call. A large result
  is read through the Storage Read API as well.

[Table reads or queries](./table-reads-or-queries.md) compares the two, with billing hints and
which one to use when.
