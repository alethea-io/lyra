# Lyra

## About

Lyra is a indexing engine for the Cardano blockchain. It is a fork of [Scrolls](https://github.com/txpipe/scrolls) and compatible with the [Quill](https://github.com/alethea-io/quill) javascript reducers.

## Usage

### Cargo Run

```bash
cargo run --bin lyra -- daemon --config examples/deno-postgres/daemon.toml
```

### Build Docker Image

```bash
docker build -t lyra:v0.1.0 .
```

### Run example

```bash
cd examples/deno-postgres
docker compose up -d
```