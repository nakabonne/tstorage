# tstorage
TStorage is a fast local on-disk storage package for time-series data with a straightforward API.
It is massively optimized ingestion as it allows the database to slice data extremely efficiently in small chunks and process it all in parallel.

## Internal
TStorage's data model differs from the B-trees or LSM trees based storage engines.
