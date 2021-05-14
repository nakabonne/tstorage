# tstorage
TStorage is a Go package for fast on-disk/in-memory time series database with a really simple API.
It is massively optimized ingestion as it allows the database to slice data extremely efficiently in small chunks and process it all in parallel.

## Internal
TStorage's data model differs from the B-trees or LSM trees based storage engines.
