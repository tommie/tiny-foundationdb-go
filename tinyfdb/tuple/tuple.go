/*
 * tuple.go
 *
 * This source file was part of the FoundationDB open source project
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

// FoundationDB Go Tuple Layer

// Package tuple provides a layer for encoding and decoding multi-element tuples
// into keys usable by FoundationDB. The encoded key maintains the same sort
// order as the original tuple: sorted first by the first element, then by the
// second element, etc. This makes the tuple layer ideal for building a variety
// of higher-level data models.
//
// For general guidance on tuple usage, see the Tuple section of Data Modeling
// (https://apple.github.io/foundationdb/data-modeling.html#tuples).
//
// FoundationDB tuples can currently encode byte and unicode strings, integers,
// large integers, floats, doubles, booleans, UUIDs, tuples, and NULL values.
// In Go these are represented as []byte (or tinyfdb.KeyConvertible), string,
// int64 (or int, uint, uint64), *big.Int (or big.Int), float32, float64, bool,
// UUID, Tuple, and nil.
package tuple

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tommie/tiny-foundationdb-go/tinyfdb"
	"github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"
)

// UUID wraps a basic byte array as a UUID. We do not provide any special
// methods for accessing or generating the UUID, but as Go does not provide
// a built-in UUID type, this simple wrapper allows for other libraries
// to write the output of their UUID type as a 16-byte array into
// an instance of this type.
type UUID = internal.UUID

// Versionstamp is struct for a FoundationDB verionstamp. Versionstamps are
// 12 bytes long composed of a 10 byte transaction version and a 2 byte user
// version. The transaction version is filled in at commit time and the user
// version is provided by the application to order results within a transaction.
type Versionstamp = internal.Versionstamp

// IncompleteVersionstamp is the constructor you should use to make
// an incomplete versionstamp to use in a tuple.
func IncompleteVersionstamp(userVersion uint16) Versionstamp {
	return Versionstamp{
		TransactionVersion: internal.IncompleteTransactionVersion,
		UserVersion:        userVersion,
	}
}

// A TupleElement is one of the types that may be encoded in FoundationDB
// tuples. Although the Go compiler cannot enforce this, it is a programming
// error to use an unsupported types as a TupleElement (and will typically
// result in a runtime panic).
//
// The valid types for TupleElement are []byte (or tinyfdb.KeyConvertible),
// string, int64 (or int, uint, uint64), *big.Int (or big.Int), float, double,
// bool, UUID, Tuple, and nil.
type TupleElement = internal.TupleElement

// Tuple is a slice of objects that can be encoded as FoundationDB tuples. If
// any of the TupleElements are of unsupported types, a runtime panic will occur
// when the Tuple is packed.
//
// Given a Tuple T containing objects only of these types, then T will be
// identical to the Tuple returned by unpacking the byte slice obtained by
// packing T (modulo type normalization to []byte, uint64, and int64).
type Tuple internal.Tuple

func (t Tuple) TupleElements() []TupleElement {
	return t
}

// String implements the fmt.Stringer interface and returns human-readable
// string representation of this tuple. For most elements, we use the
// object's default string representation.
func (tuple Tuple) String() string {
	sb := strings.Builder{}
	printTuple(tuple, &sb)
	return sb.String()
}

func printTuple(tuple Tuple, sb *strings.Builder) {
	sb.WriteString("(")

	for i, t := range tuple {
		switch t := t.(type) {
		case Tuple:
			printTuple(t, sb)
		case nil:
			sb.WriteString("<nil>")
		case string:
			sb.WriteString(strconv.Quote(t))
		case UUID:
			sb.WriteString("UUID(")
			sb.WriteString(t.String())
			sb.WriteString(")")
		case []byte:
			sb.WriteString("b\"")
			sb.WriteString(internal.ByteSliceString(t))
			sb.WriteString("\"")
		default:
			// For user-defined and standard types, we use standard Go
			// printer, which itself uses Stringer interface.
			fmt.Fprintf(sb, "%v", t)
		}

		if i < len(tuple)-1 {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(")")
}

// Unpack returns the tuple encoded by the provided byte slice, or an error if
// the key does not correctly encode a FoundationDB tuple.
func Unpack(b []byte) (Tuple, error) {
	t, err := internal.UnpackTuple(b)
	return Tuple(t), err
}

// Pack returns a new byte slice encoding the provided tuple. Pack will panic if
// the tuple contains an element of any type other than []byte,
// tinyfdb.KeyConvertible, string, int64, int, uint64, uint, *big.Int, big.Int, float32,
// float64, bool, tuple.UUID, tuple.Versionstamp, nil, or a Tuple with elements of
// valid types. It will also panic if an integer is specified with a value outside
// the range [-2**2040+1, 2**2040-1]
//
// Tuple satisfies the tinyfdb.KeyConvertible interface, so it is not necessary to
// call Pack when using a Tuple with a FoundationDB API function that requires a
// key.
//
// This method will panic if it contains an incomplete Versionstamp. Use
// PackWithVersionstamp instead.
//
func (t Tuple) Pack() []byte {
	return internal.Tuple(t).Pack()
}

// PackWithVersionstamp packs the specified tuple into a key for versionstamp
// operations. See Pack for more information. This function will return an error
// if you attempt to pack a tuple with more than one versionstamp. This function will
// return an error if you attempt to pack a tuple with a versionstamp position larger
// than an uint16 if the API version is less than 520.
func (t Tuple) PackWithVersionstamp(prefix []byte) ([]byte, error) {
	return internal.Tuple(t).PackWithVersionstamp(prefix)
}

// HasIncompleteVersionstamp determines if there is at least one incomplete
// versionstamp in a tuple. This function will return an error this tuple has
// more than one versionstamp.
func (t Tuple) HasIncompleteVersionstamp() (bool, error) {
	return internal.Tuple(t).HasIncompleteVersionstamp()
}

// FDBKey returns the packed representation of a Tuple, and allows Tuple to
// satisfy the tinyfdb.KeyConvertible interface. FDBKey will panic in the same
// circumstances as Pack.
func (t Tuple) FDBKey() tinyfdb.Key {
	return t.Pack()
}

// FDBRangeKeys allows Tuple to satisfy the tinyfdb.ExactRange interface. The range
// represents all keys that encode tuples strictly starting with a Tuple (that
// is, all tuples of greater length than the Tuple of which the Tuple is a
// prefix).
func (t Tuple) FDBRangeKeys() (tinyfdb.KeyConvertible, tinyfdb.KeyConvertible) {
	p := t.Pack()
	return tinyfdb.Key(concat(p, 0x00)), tinyfdb.Key(concat(p, 0xFF))
}

// FDBRangeKeySelectors allows Tuple to satisfy the tinyfdb.Range interface. The
// range represents all keys that encode tuples strictly starting with a Tuple
// (that is, all tuples of greater length than the Tuple of which the Tuple is a
// prefix).
func (t Tuple) FDBRangeKeySelectors() (tinyfdb.Selectable, tinyfdb.Selectable) {
	b, e := t.FDBRangeKeys()
	return tinyfdb.FirstGreaterOrEqual(b), tinyfdb.FirstGreaterOrEqual(e)
}

func concat(a []byte, b ...byte) []byte {
	r := make([]byte, len(a)+len(b))
	copy(r, a)
	copy(r[len(a):], b)
	return r
}
