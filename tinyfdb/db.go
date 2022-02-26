package tinyfdb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/tidwall/btree"
)

type Database struct {
	*database
}

func MustOpenDefault() Database {
	return Database{newDatabase()}
}

func OpenDefault() (Database, error) {
	return MustOpenDefault(), nil
}

func (d Database) CreateTransaction() (Transaction, error) {
	return d.database.CreateTransaction()
}

func (d Database) Transact(f func(Transaction) (interface{}, error)) (_ interface{}, rerr error) {
	tx, err := d.database.CreateTransaction()
	if err != nil {
		return nil, err
	}
	defer func() {
		if rerr == nil {
			rerr = tx.Commit().Get()
		}
		if rerr != nil {
			tx.Cancel()
		}
	}()

	for i := 0; ; i++ {
		v, err := f(tx)
		if err == nil {
			return v, nil
		}

		var rerr RetryableError
		if i == maxTransactRetries-1 || !errors.As(err, &rerr) {
			return nil, err
		}
	}
}

const maxTransactRetries = 10

type database struct {
	mu      sync.Mutex
	bt      *btree.BTree // keyValue
	txmap   map[*transaction]struct{}
	prevSeq uint64
}

type keyValue struct {
	Key   [][]byte
	Value []byte
}

func newDatabase() *database {
	return &database{
		bt:      btree.NewNonConcurrent(btreeBefore),
		txmap:   map[*transaction]struct{}{},
		prevSeq: 1,
	}
}

func btreeBefore(a, b interface{}) bool {
	aa := toKey(a)
	bb := toKey(b)

	n := len(aa)
	if n > len(bb) {
		n = len(bb)
	}

	for i := 0; i < n; i++ {
		v := bytes.Compare(aa[i], bb[i])
		if v != 0 {
			return v < 0
		}
	}

	return len(aa) < len(bb)
}

func toKey(v interface{}) [][]byte {
	if vv, ok := v.([][]byte); ok {
		return vv
	}
	if kv, ok := v.(keyValue); ok {
		return kv.Key
	}
	panic(fmt.Errorf("unknown key type: %T", v))
}

func (d *database) CreateTransaction() (Transaction, error) {
	t := newTransaction(d)

	d.mu.Lock()
	d.txmap[t] = struct{}{}
	d.mu.Unlock()

	return Transaction{t}, nil
}
