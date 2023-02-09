package tinyfdb

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/tidwall/btree"
	"github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"
)

// Transaction is a lightweight handle of a transaction. It is cheap
// to copy, and all copies point to the same transaction. Instances
// are goroutine-compatible, but not goroutine-safe.
type Transaction struct {
	*transaction
}

func (t Transaction) Cancel()           { t.transaction.Cancel() }
func (t Transaction) Commit() FutureNil { return t.transaction.Commit() }

func (t Transaction) Get(key KeyConvertible) FutureByteSlice { return t.transaction.Get(key) }

func (t Transaction) GetRange(r Range, opts RangeOptions) RangeResult {
	return t.transaction.GetRange(r, opts)
}

func (t Transaction) Set(key KeyConvertible, value []byte) { t.transaction.Set(key, value) }

type transaction struct {
	d *database

	mu          sync.Mutex
	taints      map[string]taintType // Mutex: d.mu
	taintStacks map[string][]string  // Mutex: d.mu
	writes      *btree.BTree
	readSeq     uint64
}

type taintType int

const (
	noTaint   taintType = 0
	readTaint taintType = 1 << iota
	writeTaint
	conflictTaint
)

func (t taintType) String() string {
	switch t {
	case noTaint:
		return "-"
	case readTaint:
		return "read"
	case writeTaint:
		return "write"
	case conflictTaint:
		return "conflict"
	default:
		return "<unknown>"
	}
}

func newTransaction(d *database) *transaction {
	return &transaction{
		d:           d,
		taints:      map[string]taintType{},
		taintStacks: map[string][]string{},
		writes:      btree.NewNonConcurrent(btreeBefore),
	}
}

func (t *transaction) Cancel() {
	t.d.mu.Lock()
	defer t.d.mu.Unlock()

	delete(t.d.txmap, t)
}

func (t *transaction) Commit() FutureNil {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.writes.Len() == 0 {
		t.Cancel()
		return &futureNil{}
	}

	t.d.mu.Lock()
	defer t.d.mu.Unlock()

	for key, taint := range t.taints {
		if taint&conflictTaint == 0 {
			continue
		}
		if taint&^conflictTaint != 0 {
			var k interface{} = []byte(key)
			if kt, err := internal.UnpackTuple([]byte(key)); err == nil {
				k = kt
			}

			if t.d.raceStacks != nil {
				fmt.Fprintf(t.d.raceStacks, "*** TinyFDB Races for key %+v ***\n", k)
				for _, stack := range t.taintStacks[key] {
					fmt.Fprintln(t.d.raceStacks, "Race", stack)
				}
			}

			return &futureNil{err: RetryableError{fmt.Errorf("write race for key %+v", k)}}
		}
	}

	for key, taint := range t.taints {
		if taint&writeTaint == 0 {
			continue
		}
		for t2 := range t.d.txmap {
			if t2 == t {
				continue
			}
			t2.taints[key] |= conflictTaint
			if t.d.raceStacks != nil {
				t2.taintStacks[key] = append(t2.taintStacks[key], t.taintStacks[key]...)
			}
		}
	}

	t.d.prevSeq++
	if t.d.prevSeq == 0 {
		panic(fmt.Errorf("tinyfdb/database.prevSeq wrapped around"))
	}

	var hint btree.PathHint
	t.writes.Ascend(nil, func(item interface{}) bool {
		kv := item.(keyValue)
		kv.Key = append(append(internal.Tuple{}, kv.Key...), t.d.prevSeq)
		t.d.bt.SetHint(kv, &hint)
		return true
	})

	delete(t.d.txmap, t)

	return &futureNil{}
}

func (t *transaction) ClearRange(er ExactRange) {
	b, e := er.FDBRangeKeys()

	bb, err := internal.UnpackTuple(b.FDBKey())
	if err != nil {
		return
	}

	ee, err := internal.UnpackTuple(e.FDBKey())
	if err != nil {
		return
	}

	t.ascend(bb, func(kv keyValue) bool {
		// t.d.mu already locked.

		c, err := kv.Key[:len(kv.Key)-1].Cmp(ee)
		if err != nil {
			panic(err)
		}
		if c >= 0 {
			return false
		}
		k := kv.Key[:len(kv.Key)-1]
		kbs := k.Pack()
		if kv.Value != nil {
			t.writes.Set(keyValue{k, nil})
			t.setTaintLocked(kbs, writeTaint, 0)
		} else {
			// A tombstone means we shouldn't taint this. We may have
			// done so on earlier versions already. Conflicts with
			// other transactions must be preserved.
			t.writes.Delete(keyValue{k, nil})
			taint := t.taints[string(kbs)] & ^(readTaint | writeTaint)
			if taint == 0 {
				delete(t.taints, string(kbs))
			} else {
				t.taints[string(kbs)] = taint
			}
		}
		return true
	})
}

func (t *transaction) Get(key KeyConvertible) FutureByteSlice {
	k, err := internal.UnpackTuple(key.FDBKey())
	if err != nil {
		return &futureByteSlice{err: err}
	}

	var found *keyValue
	var ferr error
	t.ascend(k, func(kv keyValue) bool {
		c, err := kv.Key[:len(kv.Key)-1].Cmp(k)
		if err != nil {
			ferr = err
			return false
		}
		if c != 0 {
			return false
		}
		found = &kv
		return true
	})

	if ferr != nil {
		return &futureByteSlice{err: ferr}
	}
	if found == nil {
		return &futureByteSlice{}
	}
	t.setTaint(found.Key.Pack(), readTaint)
	return &futureByteSlice{bs: found.Value}
}

func (t *transaction) GetRange(r Range, opts RangeOptions) RangeResult {
	begin, end := r.FDBRangeKeySelectors()
	t.getReadSeq()
	return newRangeResult(t, begin.FDBKeySelector(), end.FDBKeySelector(), opts)
}

func (t *transaction) getReadSeq() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.readSeq == 0 {
		t.d.mu.Lock()
		defer t.d.mu.Unlock()

		t.readSeq = t.d.prevSeq
	}
	return t.readSeq
}

func (t *transaction) ascend(pivot internal.Tuple, fun func(keyValue) bool) {
	seq := t.getReadSeq()

	t.d.mu.Lock()
	defer t.d.mu.Unlock()

	t.d.bt.Ascend(pivot, func(item interface{}) bool {
		kv := item.(keyValue)
		if kv.Key[len(kv.Key)-1].(uint64) > seq {
			return true
		}

		return fun(kv)
	})
}

func (t *transaction) descend(pivot internal.Tuple, fun func(keyValue) bool) {
	seq := t.getReadSeq()

	t.d.mu.Lock()
	defer t.d.mu.Unlock()

	t.d.bt.Descend(pivot, func(item interface{}) bool {
		kv := item.(keyValue)
		if kv.Key[len(kv.Key)-1].(uint64) > seq {
			return true
		}

		return fun(kv)
	})
}

func (t *transaction) Set(key KeyConvertible, value []byte) {
	k := key.FDBKey()
	t.setTaint(k, writeTaint)

	kt, err := internal.UnpackTuple(k)
	if err != nil {
		panic(err)
	}
	t.writes.Set(keyValue{kt, value})
}

func (t *transaction) setTaint(key []byte, typ taintType) {
	t.d.mu.Lock()
	t.setTaintLocked(key, typ, 1)
	t.d.mu.Unlock()
}

func (t *transaction) setTaintLocked(key []byte, typ taintType, stackSkip int) {
	sk := string(key)

	t.taints[sk] |= typ

	if t.d.raceStacks != nil {
		t.taintStacks[sk] = append(t.taintStacks[sk], fmt.Sprintf("%s in %s", typ & ^conflictTaint, stackTrace(1+stackSkip)))
	}
}

func stackTrace(skip int) string {
	// It would be nice to use runtime.Callers here, which allows us
	// to skip frames. But we'd still like to have the goroutine
	// identifier.
	s := strings.TrimSpace(string(debug.Stack()))
	var hdr string
	for i := 0; s != "" && i < 1+2*(2+skip); i++ {
		l, tail, _ := strings.Cut(s, "\n")
		s = tail

		if i == 0 {
			hdr = l
		}
	}

	return hdr + "\n" + s
}
