# Tiny FoundationDB for Go

This is an in-memory implementation of the
[FoundationDB](https://www.foundationdb.org/)
[`fdb` package](https://pkg.go.dev/github.com/apple/foundationdb/bindings/go/src/fdb). It
is written in [Go](https://go.dev/) and does not depend on `fdb` or
cgo. The purpose of this library is to quickly use FoundationDB in
small integration tests, and maybe even unit tests.

Do not use this code in production; that's what FoundationDB proper is
for. Do use this for speeding up small tests.

## Usage

This is meant as a drop-in replacement for `fdb`:

```go
package main

import fdb "github.com/tommie/tuny-foundationdb-go/tinyfdb"

func main() {
    fdb := fdb.MustOpenDefault()
    // ...
}
```

Due to the heavy use of opaque structs in the upstream API, it is not
possible to make this a runtime-switchable plug-in. The source is
compatible, but binaries are not. One solution to this problem is to
generate a `tinyfdb` version of the source files that import `fdb`,
and perhaps use build flags to select the implementation.

## Current Status

This is far from a complete implementation of the API. The idea was/is
to implement features as and when they are needed, but ensuring that
the features that do exist work well sanely.

[x] `APIVersion` et al.
[x] `Database.CreateTransaction`
[x] `Database.Transact`
[x] `Transaction.ClearRange`
[x] `Transaction.Get`
[x] `Transaction.GetRange`
[ ] `Transaction.GetRange` with `RangeOptions`
[x] `Transaction.Set`
[x] Tuple keys and the `tuple` package

### Implementation Notes

This library is backed by
[`tidwall/btree`](https://pkg.go.dev/github.com/tidwall/btree) and
uses MVCC-style sequence numbers for transaction guarantees.

Compaction is not implemented, which means queries will be slower and
slower. Only use it for short tests.

## License

Unless otherwise noted in each file, this code is distributed under
the MIT license. See the LICENSE file.

Some files are copies from the apple/foundationdb repository, and they
are licensed under the Apache license, as indicated in file headers.
