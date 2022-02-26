package tinyfdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type RangeResult struct {
	t          rangeResultTx
	seqBS      []byte
	begin, end keySelector
}

type rangeResultTx interface {
	ascend([][]byte, func(keyValue) bool)
	setTaint([][]byte, taintType)
}

func newRangeResult(t rangeResultTx, seq uint64, b, e KeySelector) RangeResult {
	seqBS := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBS, seq)

	return RangeResult{
		t: t,
		begin: keySelector{
			// TODO: decode key
			Key:     [][]byte{[]byte(b.Key.FDBKey())},
			OrEqual: b.OrEqual,
			Offset:  b.Offset,
		},
		end: keySelector{
			// TODO: decode key
			Key:     [][]byte{[]byte(e.Key.FDBKey())},
			OrEqual: e.OrEqual,
			Offset:  e.Offset,
		},
		seqBS: seqBS,
	}
}

func (rr RangeResult) Iterator() *RangeIterator {
	return &RangeIterator{
		rr:   rr,
		next: keyMatcher{sel: rr.begin},
		end:  keyMatcher{sel: rr.end},
	}
}

type RangeIterator struct {
	kv keyValue

	rr   RangeResult
	next keyMatcher
	end  keyMatcher
}

func (ri *RangeIterator) Advance() bool {
	var found bool
	ri.rr.t.ascend(ri.next.sel.Key, func(kv keyValue) bool {
		if bytes.Compare(kv.Key[len(kv.Key)-1], ri.rr.seqBS) > 0 {
			return true
		}

		if ri.end.Match(kv.Key[:len(kv.Key)-1]) != noMatch {
			return false
		}

		switch ri.next.Match(kv.Key[:len(kv.Key)-1]) {
		case noMatch:
			ri.kv = kv
			return true

		case matchCurrent:
			ri.kv = kv
		}

		ri.rr.t.setTaint(ri.kv.Key, readTaint)
		found = true
		return false
	})

	if found || ri.next.End() == matchPrev {
		// Add an empty field so we don't look at the same item
		// again. This is like firstGreaterThan, but is a bit less
		// verbose in tests.
		k := make([][]byte, len(ri.kv.Key)+1)
		copy(k, ri.kv.Key)
		ri.next = keyMatcher{sel: firstGreaterOrEqual(k)}
		return true
	}

	return false
}

func (ri *RangeIterator) Get() (KeyValue, error) {
	// TODO: encode key
	return KeyValue{Key: ri.kv.Key[0], Value: ri.kv.Value}, nil
}

type keySelector struct {
	Key     [][]byte
	OrEqual bool
	Offset  int
}

func firstGreaterOrEqual(key [][]byte) keySelector { return keySelector{key, false, 1} }

// A keyMatcher is a stateful matcher for a `keySelector`. For many
// selectors (last-of), this needs a one item look-ahead, which means
// it can return the result "use the previous item."
//
// This is goroutine-compatible.
type keyMatcher struct {
	sel     keySelector
	i       int
	hasPrev bool
}

// Match expects a series of non-decreasing keys. After the first
// non-`noMatch`, no more calls to `Match` should be made. If `Match`
// still hasn't returned a match at the end of the stream of keys,
// `End` should be called.
func (m *keyMatcher) Match(k [][]byte) matchResult {
	var cmp int
	if btreeBefore(k, m.sel.Key) {
		// Key is earlier than selector.
		cmp = -1
	} else if btreeBefore(m.sel.Key, k) {
		// Key is later than selector.
		cmp = 1
	}

	if m.sel.Offset == 0 {
		// We're looking for the last earlier (or same).

		m.hasPrev = cmp < 0 || (m.sel.OrEqual && cmp == 0)
		if m.hasPrev {
			return noMatch
		}
		return matchPrev
	} else {
		// We're looking for the first later (or same).

		if cmp < 0 || (m.sel.OrEqual && cmp == 0) {
			return noMatch
		}

		m.i++
		if m.i < m.sel.Offset {
			return noMatch
		}
		return matchCurrent
	}
}

// End is called after the last key has been fed to `Match` and there
// has still not been a match. It may return `matchPrev`, but not
// `matchCurrent`.
func (m *keyMatcher) End() matchResult {
	if m.hasPrev {
		m.hasPrev = false
		return matchPrev
	}
	return noMatch
}

type matchResult int

const (
	noMatch matchResult = iota
	matchPrev
	matchCurrent
)

func (r matchResult) String() string {
	switch r {
	case noMatch:
		return "noMatch"
	case matchPrev:
		return "matchPrev"
	case matchCurrent:
		return "matchCurrent"
	default:
		return fmt.Sprint(int(r))
	}
}
