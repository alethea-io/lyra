# BuiltIn Reducers

The builtin reducer allows users to write reducer logic in Rust which can then be compiled with the Lyra binary. Currently only one reducer has been written (`full_utxos_by_address`) to serve as an example for developers.

## Configuration

Example of a configuration

```toml
[reducer]
type = "BuiltIn"

[[reducer.reducers]]
type = "FullUtxosByAddress"
filter = ["addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha"]
```

### Section: `reducer`

- `type`: the literal value `BuiltIn`.
- `reducers`: a list of reducer configurations 