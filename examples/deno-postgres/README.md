## Setup Postgres DB
```bash
docker compose up -d
```

## Run lyra
```bash
RUST_BACKTRACE=1 cargo run --bin lyra --features deno -- daemon --config examples/deno-postgres/daemon.toml
```