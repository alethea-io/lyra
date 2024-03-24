# Lyra

## About

Lyra is a indexing engine for the Cardano blockchain. It is a fork of [Scrolls](https://github.com/txpipe/scrolls) and compatible with the [Quill](https://github.com/alethea-io/quill) javascript reducers.

## Usage

### Build Docker Image

```bash
docker build -t lyra .
```

### Run example

```bash
cd examples/deno-postgres
docker compose up -d
```

## TODO
- Panic if disconnected from redis db