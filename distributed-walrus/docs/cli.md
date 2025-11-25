# Walrus CLI

Small helper for talking to a running distributed-walrus node over its TCP client port.

## Build

```
cargo build --bin walrus-cli --manifest-path distributed-walrus/Cargo.toml
```

## Run

The client defaults to `127.0.0.1:8080`. Override with `--addr HOST:PORT`.

### Interactive shell (default)

Start without a subcommand to drop into a REPL:

```
cargo run --bin walrus-cli --manifest-path distributed-walrus/Cargo.toml --
```

Type commands; `exit/quit/q` or Ctrl+C to leave. Examples inside the shell:

```
REGISTER logs
PUT logs hello
GET logs
STATE logs
METRICS
```

### One-off commands

- Register a topic (idempotent):

```
cargo run --bin walrus-cli --manifest-path distributed-walrus/Cargo.toml -- register logs
```

- Append data:

```
cargo run --bin walrus-cli --manifest-path distributed-walrus/Cargo.toml -- put logs "hello world"
```

- Read one entry (advances shared cursor; prints `EMPTY` if none):

```
cargo run --bin walrus-cli --manifest-path distributed-walrus/Cargo.toml -- get logs
```

- Inspect topic state (JSON):

```
cargo run --bin walrus-cli --manifest-path distributed-walrus/Cargo.toml -- state logs
```

- Raft/metrics snapshot (JSON):

```
cargo run --bin walrus-cli --manifest-path distributed-walrus/Cargo.toml -- metrics
```

## Protocol Notes

Commands speak the simple length-prefixed text protocol exposed by the node’s TCP listener:

```
REGISTER <topic>
PUT <topic> <payload>
GET <topic>
STATE <topic>
METRICS
```

Success replies are `OK` or `OK <payload>`. `GET` returns `EMPTY` when no data is available. Errors are returned as `ERR ...` and surfaced by the CLI with a non-zero exit.*** End Patch
