# redis-server

A simple Redis server written in Rust using [Tokio](https://tokio.rs/)

## Building

To build everything:

```shell
cargo build
```

To build a specific package:

```shell
cargo build -p <package>

cargo build -p resp
```

## Tests

To run the tests:

```shell
# run all tests
cargo test

# run tests for specific package
cargo test -p <package>
```

## Running

To run the server:

```shell
cargo run -p resp-server
```

