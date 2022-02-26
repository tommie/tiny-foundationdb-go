/* Definitions from github.com/apple/foundationdb/bindings/go/src/fdb.
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tinyfdb

import "github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"

type ExactRange interface {
	// FDBRangeKeys returns a pair of keys that describe the beginning and end
	// of a range.
	FDBRangeKeys() (begin, end KeyConvertible)

	// An object that implements ExactRange must also implement Range
	// (logically, by returning FirstGreaterOrEqual of the keys returned by
	// FDBRangeKeys).
	Range
}

type Future interface {
	// BlockUntilReady blocks the calling goroutine until the future is ready. A
	// future becomes ready either when it receives a value of its enclosed type
	// (if any) or is set to an error state.
	BlockUntilReady()

	// IsReady returns true if the future is ready, and false otherwise, without
	// blocking. A future is ready either when has received a value of its
	// enclosed type (if any) or has been set to an error state.
	IsReady() bool

	// Cancel cancels a future and its associated asynchronous operation. If
	// called before the future becomes ready, attempts to access the future
	// will return an error. Cancel has no effect if the future is already
	// ready.
	//
	// Note that even if a future is not ready, the associated asynchronous
	// operation may already have completed and be unable to be cancelled.
	Cancel()
}

type FutureByteSlice interface {
	// Get returns a database value (or nil if there is no value), or an error
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	Get() ([]byte, error)

	// MustGet returns a database value (or nil if there is no value), or panics
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	MustGet() []byte

	Future
}

type FutureInt64 interface {
	// Get returns a database version or an error if the asynchronous operation
	// associated with this future did not successfully complete. The current
	// goroutine will be blocked until the future is ready.
	Get() (int64, error)

	// MustGet returns a database version, or panics if the asynchronous
	// operation associated with this future did not successfully complete. The
	// current goroutine will be blocked until the future is ready.
	MustGet() int64

	Future
}

type FutureNil interface {
	// Get returns an error if the asynchronous operation associated with this
	// future did not successfully complete. The current goroutine will be
	// blocked until the future is ready.
	Get() error

	// MustGet panics if the asynchronous operation associated with this future
	// did not successfully complete. The current goroutine will be blocked
	// until the future is ready.
	MustGet()

	Future
}

type Key = internal.Key

type KeyConvertible = internal.KeyConvertible

type KeyRange struct {
	// The (inclusive) beginning of the range
	Begin KeyConvertible

	// The (exclusive) end of the range
	End KeyConvertible
}

// FDBRangeKeys allows KeyRange to satisfy the ExactRange interface.
func (kr KeyRange) FDBRangeKeys() (KeyConvertible, KeyConvertible) {
	return kr.Begin, kr.End
}

// FDBRangeKeySelectors allows KeyRange to satisfy the Range interface.
func (kr KeyRange) FDBRangeKeySelectors() (Selectable, Selectable) {
	return FirstGreaterOrEqual(kr.Begin), FirstGreaterOrEqual(kr.End)
}

type KeySelector struct {
	Key     KeyConvertible
	OrEqual bool
	Offset  int
}

// LastLessThan returns the KeySelector specifying the lexigraphically greatest
// key present in the database which is lexigraphically strictly less than the
// given key.
func LastLessThan(key KeyConvertible) KeySelector {
	return KeySelector{key, false, 0}
}

// LastLessOrEqual returns the KeySelector specifying the lexigraphically
// greatest key present in the database which is lexigraphically less than or
// equal to the given key.
func LastLessOrEqual(key KeyConvertible) KeySelector {
	return KeySelector{key, true, 0}
}

// FirstGreaterThan returns the KeySelector specifying the lexigraphically least
// key present in the database which is lexigraphically strictly greater than
// the given key.
func FirstGreaterThan(key KeyConvertible) KeySelector {
	return KeySelector{key, true, 1}
}

// FirstGreaterOrEqual returns the KeySelector specifying the lexigraphically
// least key present in the database which is lexigraphically greater than or
// equal to the given key.
func FirstGreaterOrEqual(key KeyConvertible) KeySelector {
	return KeySelector{key, false, 1}
}

func (s KeySelector) FDBKeySelector() KeySelector {
	return s
}

type KeyValue struct {
	Key   Key
	Value []byte
}

type Range interface {
	// FDBRangeKeySelectors returns a pair of key selectors that describe the
	// beginning and end of a range.
	FDBRangeKeySelectors() (begin, end Selectable)
}

type RangeOptions struct {
	// Limit restricts the number of key-value pairs returned as part of a range
	// read. A value of 0 indicates no limit.
	Limit int

	// Mode sets the streaming mode of the range read, allowing the database to
	// balance latency and bandwidth for this read.
	Mode StreamingMode

	// Reverse indicates that the read should be performed in lexicographic
	// (false) or reverse lexicographic (true) order. When Reverse is true and
	// Limit is non-zero, the last Limit key-value pairs in the range are
	// returned. Reading ranges in reverse is supported natively by the
	// database and should have minimal extra cost.
	Reverse bool
}

type Selectable interface {
	FDBKeySelector() KeySelector
}

type SelectorRange struct {
	Begin, End Selectable
}

func (r SelectorRange) FDBRangeKeySelectors() (begin, end Selectable) {
	return r.Begin, r.End
}

type StreamingMode int

const (
	// Client intends to consume the entire range and would like it all
	// transferred as early as possible.
	StreamingModeWantAll StreamingMode = -1

	// The default. The client doesn't know how much of the range it is likely
	// to used and wants different performance concerns to be balanced. Only a
	// small portion of data is transferred to the client initially (in order to
	// minimize costs if the client doesn't read the entire range), and as the
	// caller iterates over more items in the range larger batches will be
	// transferred in order to minimize latency. After enough iterations, the
	// iterator mode will eventually reach the same byte limit as “WANT_ALL“
	StreamingModeIterator StreamingMode = 0

	// Infrequently used. The client has passed a specific row limit and wants
	// that many rows delivered in a single batch. Because of iterator operation
	// in client drivers make request batches transparent to the user, consider
	// “WANT_ALL“ StreamingMode instead. A row limit must be specified if this
	// mode is used.
	StreamingModeExact StreamingMode = 1

	// Infrequently used. Transfer data in batches small enough to not be much
	// more expensive than reading individual rows, to minimize cost if
	// iteration stops early.
	StreamingModeSmall StreamingMode = 2

	// Infrequently used. Transfer data in batches sized in between small and
	// large.
	StreamingModeMedium StreamingMode = 3

	// Infrequently used. Transfer data in batches large enough to be, in a
	// high-concurrency environment, nearly as efficient as possible. If the
	// client stops iteration early, some disk and network bandwidth may be
	// wasted. The batch size may still be too small to allow a single client to
	// get high throughput from the database, so if that is what you need
	// consider the SERIAL StreamingMode.
	StreamingModeLarge StreamingMode = 4

	// Transfer data in batches large enough that an individual client can get
	// reasonable read bandwidth from the database. If the client stops
	// iteration early, considerable disk and network bandwidth may be wasted.
	StreamingModeSerial StreamingMode = 5
)
