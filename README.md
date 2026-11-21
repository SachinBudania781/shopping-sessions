# Shopping Sessions

A Rust implementation of the Databricks Shopping Sessions challenge. It reads the mounted CSV files, resolves linked shoppers, and writes the user and campaign daily tables as Parquet.

```sh
docker build --platform linux/amd64 -t sessions .
mkdir -p out && chmod 777 out
docker run --rm --network none --read-only --user 1000:1000 \
  -v "$PWD/data:/data:ro" -v "$PWD/out:/output" sessions
```

Use an empty output directory for each dataset. The binary reads `DATA_DIR`, `OUTPUT_DIR`, and `NCPU`, which default to `/data`, `/output`, and `8`.

Event copies are partitioned by event ID before selecting the earliest ingest, including invalid winners. Surviving events and valid clicks are then partitioned by canonical shopper. A time-ordered scan builds sessions and maintains the set of campaigns active in each seven-day attribution window.

For local verification, build with `cargo build --release --locked`. The official starter's `scripts/run_fixtures.py` accepts the resulting binary after `--`. Its `scripts/compare.py` compares the resulting Parquet directories with the published development answers.
