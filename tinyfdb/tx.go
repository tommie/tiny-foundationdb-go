package tinyfdb

import (
	"fmt"
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

	mu      sync.Mutex
	taints  map[string]taintType // Mutex: d.mu
	writes  *btree.BTree
	readSeq uint64
}

type taintType int

const (
	readTaint taintType = iota
	writeTaint
)

func newTransaction(d *database) *transaction {
	return &transaction{
		d:      d,
		taints: map[string]taintType{},
		writes: btree.NewNonConcurrent(btreeBefore),
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

	for key := range t.taints {
		for t2 := range t.d.txmap {
			if t2 == t {
				continue
			}
			ok := t2.hasWriteTaintLocked(key)
			if ok {
				return &futureNil{err: RetryableError{fmt.Errorf("write race with transaction %p", t2)}}
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

// hasWriteTaintLocked checks if the transaction has written to
// key. t.d.mu must be locked.
func (t *transaction) hasWriteTaintLocked(key string) bool {
	typ, ok := t.taints[key]
	return ok && typ == writeTaint
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
		if kv.Value != nil {
			t.writes.Set(keyValue{k, nil})
			t.taints[string(k.Pack())] = writeTaint
		} else {
			// A tombstone means we shouldn't taint this. We may have
			// done so on earlier versions already.
			t.writes.Delete(keyValue{k, nil})
			delete(t.taints, string(k.Pack()))
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
	t.setTaint(found.Key, readTaint)
	return &futureByteSlice{bs: found.Value}
}

func (t *transaction) GetRange(r Range, _ RangeOptions) RangeResult {
	begin, end := r.FDBRangeKeySelectors()
	t.getReadSeq()
	return newRangeResult(t, begin.FDBKeySelector(), end.FDBKeySelector())
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

func (t *transaction) Set(key KeyConvertible, value []byte) {
	t.d.mu.Lock()
	t.taints[string(key.FDBKey())] = writeTaint
	t.d.mu.Unlock()

	k, err := internal.UnpackTuple(key.FDBKey())
	if err != nil {
		panic(err)
	}
	t.writes.Set(keyValue{k, value})
}

func (t *transaction) setTaint(key internal.Tuple, typ taintType) {
	k := string(key.Pack())

	t.d.mu.Lock()
	defer t.d.mu.Unlock()

	// Allow upgrading readTaint to writeTaint, but no downgrade.
	if _, ok := t.taints[k]; !ok || typ == writeTaint {
		t.taints[k] = typ
	}
}
