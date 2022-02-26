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

func (t Transaction) GetRange(r Range, opts RangeOptions) RangeResult {
	return t.transaction.GetRange(r, opts)
}

func (t Transaction) Set(key KeyConvertible, value []byte) { t.transaction.Set(key, value) }

type transaction struct {
	d *database

	mu      sync.Mutex
	taints  map[string]taintType
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
			if t2.hasWriteTaint(key) {
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
		kv.Key = append(kv.Key, t.d.prevSeq)
		t.d.bt.SetHint(kv, &hint)
		return true
	})

	delete(t.d.txmap, t)

	return &futureNil{}
}

func (t *transaction) hasWriteTaint(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	typ, ok := t.taints[key]
	return ok && typ == writeTaint
}

func (t *transaction) GetRange(r Range, _ RangeOptions) RangeResult {
	begin, end := r.FDBRangeKeySelectors()
	return newRangeResult(t, t.getReadSeq(), begin.FDBKeySelector(), end.FDBKeySelector())
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
	t.d.mu.Lock()
	defer t.d.mu.Unlock()

	t.d.bt.Ascend(pivot, func(item interface{}) bool {
		return fun(item.(keyValue))
	})
}

func (t *transaction) Set(key KeyConvertible, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.taints[string(key.FDBKey())] = writeTaint
	k, err := internal.UnpackTuple(key.FDBKey())
	if err != nil {
		panic(err)
	}
	t.writes.Set(keyValue{k, value})
}

func (t *transaction) setTaint(key internal.Tuple, typ taintType) {
	k := string(key.Pack())

	t.mu.Lock()
	defer t.mu.Unlock()

	// Allow upgrading readTaint to writeTaint, but no downgrade.
	if _, ok := t.taints[k]; !ok || typ == writeTaint {
		t.taints[k] = typ
	}
}
