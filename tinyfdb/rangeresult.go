package tinyfdb

import (
	"fmt"
	"math"

	"github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"
)

type RangeResult struct {
	t          rangeResultTx
	begin, end keySelector
	opts       RangeOptions

	seq uint64
}

type rangeResultTx interface {
	ascend(internal.Tuple, func(keyValue) bool)
	descend(internal.Tuple, func(keyValue) bool)
	setTaint(internal.Tuple, taintType)
}

func newRangeResult(t rangeResultTx, b, e KeySelector, opts RangeOptions) RangeResult {
	begin, err := internal.UnpackTuple(b.Key.FDBKey())
	if err != nil {
		panic(fmt.Errorf("failed to unpack begin key: %w", err))
	}
	end, err := internal.UnpackTuple(e.Key.FDBKey())
	if err != nil {
		panic(fmt.Errorf("failed to unpack end key: %w", err))
	}

	return RangeResult{
		t: t,
		begin: keySelector{
			Key:     begin,
			OrEqual: b.OrEqual,
			Offset:  b.Offset,
		},
		end: keySelector{
			Key:     end,
			OrEqual: e.OrEqual,
			Offset:  e.Offset,
		},
		opts: opts,
	}
}

func (rr RangeResult) Iterator() *RangeIterator {
	it := &RangeIterator{
		next:   keyMatcher{sel: rr.begin, inverse: rr.opts.Reverse},
		end:    keyMatcher{sel: rr.end, inverse: rr.opts.Reverse},
		scendf: rr.t.ascend,
		rr:     rr,
	}
	if rr.opts.Reverse {
		it.next, it.end = it.end, it.next
		it.scendf = rr.t.descend
	}
	return it
}

type RangeIterator struct {
	kv keyValue

	next   keyMatcher
	end    keyMatcher
	scendf func(internal.Tuple, func(keyValue) bool)
	rr     RangeResult

	n int
}

func (ri *RangeIterator) Advance() bool {
	if n := ri.rr.opts.Limit; n > 0 && ri.n >= n {
		return false
	}

	for {
		var prev, found *keyValue
		ri.scendf(ri.next.sel.Key, func(kv keyValue) bool {
			if ri.end.Match(kv.Key[:len(kv.Key)-1]) != noMatch {
				return false
			}

			if found != nil {
				c, err := found.Key[:len(found.Key)-1].Cmp(kv.Key[:len(kv.Key)-1])
				if err != nil {
					panic(err)
				}
				if c != 0 {
					// The next item is a different key, so we've found
					// the highest sequence number.
					return false
				}
			}

			switch ri.next.Match(kv.Key[:len(kv.Key)-1]) {
			case noMatch:
				prev = &kv
				return true

			case matchPrev:
				found = prev

			case matchCurrent:
				found = &kv
			}

			// We must find the last sequence number for this key, so
			// carry on searching.
			return true
		})

		if found == nil && ri.next.End() == matchPrev {
			found = prev
		}

		if found != nil {
			if !ri.rr.opts.Reverse {
				// Set the seq to max and add an empty field so we don't look
				// at the same item again. This is like
				// firstGreaterThan(found.Key[:-1]), but is a bit less verbose
				// in tests.
				k := make(internal.Tuple, len(found.Key)+1)
				copy(k, found.Key)
				k[len(k)-1] = uint64(math.MaxUint64)
				ri.next = keyMatcher{sel: firstGreaterOrEqual(k)}
			} else {
				// For reverse iteration, we simply do
				// firstGreaterThan, but cheap by setting the offset
				// to skip the currently found key.
				ri.next = keyMatcher{sel: keySelector{found.Key, true, 2}, inverse: true}
			}

			if found.Value == nil {
				// A tombstone.
				continue
			}

			ri.rr.t.setTaint(found.Key[:len(found.Key)-1], readTaint)
			ri.kv = *found
			ri.n++
			return true
		}

		return false
	}
}

func (ri *RangeIterator) Get() (KeyValue, error) {
	return KeyValue{Key: ri.kv.Key[:len(ri.kv.Key)-1].Pack(), Value: ri.kv.Value}, nil
}

type keySelector struct {
	Key     internal.Tuple
	OrEqual bool
	Offset  int
}

func firstGreaterOrEqual(key internal.Tuple) keySelector { return keySelector{key, false, 1} }

// A keyMatcher is a stateful matcher for a `keySelector`. For many
// selectors (last-of), this needs a one item look-ahead, which means
// it can return the result "use the previous item."
//
// This is goroutine-compatible.
type keyMatcher struct {
	sel     keySelector
	i       int
	hasPrev bool
	inverse bool
}

// Match expects a series of non-decreasing keys. After the first
// non-`noMatch`, no more calls to `Match` should be made. If `Match`
// still hasn't returned a match at the end of the stream of keys,
// `End` should be called.
func (m *keyMatcher) Match(k internal.Tuple) matchResult {
	var cmp int
	if btreeBefore(k, m.sel.Key) {
		// Key is earlier than selector.
		cmp = -1
	} else if btreeBefore(m.sel.Key, k) {
		// Key is later than selector.
		cmp = 1
	}
	if m.inverse {
		cmp = -cmp
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
