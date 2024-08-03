# Deno Reducers

The deno reducer enables users to easily create javascript reducers for custom block transformation logic which then plugs into Lyra. There is an existing library of javascript reducers available in the [Quill](https://github.com/alethea-io/quill) repo.

## Configuration

Example of a configuration

```toml
[reducer]
type = "Deno"
reducer_module = "./examples/deno-postgres/reduce.js"
use_async = false
```

### Section: `reducer`

- `type`: the literal value `Deno`.
- `reducer_module`: the js file with the reducer logic
- `use_async`: run the js in async mode