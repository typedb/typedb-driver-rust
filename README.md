# THIS REPOSITORY HAS BEEN ARCHIVED. Please visit: [@vaticle/typedb-driver/rust](https://github.com/vaticle/typedb-driver/tree/development/rust)

---

# TypeDB Client for Rust

[![Factory](https://factory.vaticle.com/api/status/vaticle/typedb-client-rust/badge.svg)](https://factory.vaticle.com/vaticle/typedb-client-rust)
[![Discord](https://img.shields.io/discord/665254494820368395?color=7389D8&label=chat&logo=discord&logoColor=ffffff)](https://vaticle.com/discord)
[![Discussion Forum](https://img.shields.io/discourse/https/forum.vaticle.com/topics.svg)](https://forum.vaticle.com)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typedb-796de3.svg)](https://stackoverflow.com/questions/tagged/typedb)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typeql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/typeql)

## Project Status
This is a **work in progress** and is not yet suitable for production usage.

It can connect to TypeDB, run read and write queries, and return answers. Concept API methods are not available yet.

## Client Architecture
To learn about the mechanism that a TypeDB Client uses to set up communication with databases running on the TypeDB Server, refer to [TypeDB > Client API > Overview](http://docs.vaticle.com/docs/client-api/overview).

The TypeDB Client for Rust provides a fully async API that supports the [`tokio`](https://crates.io/crates/tokio) **multi-threaded** runtime.

## Quickstart
1. Import `typedb-client` through Cargo:
```toml
typedb-client = "0.1.2"
```
2. Make sure the [TypeDB Server](https://docs.vaticle.com/docs/running-typedb/install-and-run#start-the-typedb-server) is running.
3. See `tests/integration` for examples of usage.

## Build from Source
> Note: You don't need to compile TypeDB Client from source if you just want to use it in your code. See the _"Quickstart"_ section above.

1. Make sure you have [Bazel](https://docs.bazel.build/versions/master/install.html) installed on your machine.

2. Build the library:

   a) to build the native/raw rlib:
   ```
   bazel build //:typedb_client
   ```
   The rlib will be produced at: `bazel-bin/libtypedb_client-{hash}.rlib`.

   b) to build the crate for a Cargo project:
   ```
   bazel build //:assemble_crate
   ```
   The Cargo crate will be produced at:
   ```
   bazel-bin/assemble_crate.crate
   ```
   You can then unzip this crate to retrieve `Cargo.toml`. **Please note**: this process has not yet been thoroughly tested. The generated `Cargo.toml` may not be fully correct. See the `Cargo.toml` of the `typedb-client` crate for reference.
   
