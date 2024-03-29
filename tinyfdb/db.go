package tinyfdb

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/tidwall/btree"
	"github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"
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

// Debug is a tinyfdb extension that allows setting debug parameters.
func (d Database) Debug() *DBDebug {
	return (*DBDebug)(d.database)
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
	mu         sync.Mutex
	bt         *btree.BTree // keyValue
	txmap      map[*transaction]struct{}
	prevSeq    uint64
	raceStacks io.Writer
}

type keyValue struct {
	Key   internal.Tuple
	Value []byte
}

func newDatabase() *database {
	return &database{
		bt:         btree.NewNonConcurrent(btreeBefore),
		txmap:      map[*transaction]struct{}{},
		prevSeq:    1,
		raceStacks: defaultPrintRaceStacks(),
	}
}

func btreeBefore(a, b interface{}) bool {
	aa := toKey(a)
	bb := toKey(b)

	v, err := aa.Cmp(bb)
	if err != nil {
		panic(err)
	}
	return v < 0
}

func toKey(v interface{}) internal.Tuple {
	if vv, ok := v.(internal.Tuple); ok {
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
