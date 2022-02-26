package tinyfdb

import (
	"errors"
	"reflect"
	"testing"

	"github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"
)

func TestTransactionCommit(t *testing.T) {
	t.Run("removes", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		tx, err := db.CreateTransaction()
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		if err := tx.Commit().Get(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		if _, ok := db.database.txmap[tx.transaction]; ok {
			t.Errorf("txmap: transaction not unregistered: %v", db.database.txmap)
		}
	})

	t.Run("updates", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		wantValue := []byte("avalue")
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			tx.Set(Key(internal.Tuple{"akey"}.Pack()), wantValue)
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		db.bt.Ascend(nil, func(item interface{}) bool {
			t.Logf("Item: %+v", item)
			return true
		})

		if got, want := db.bt.Len(), 1; got != want {
			t.Errorf("Set Len: got %v, want %v", got, want)
		}

		wantKey := internal.Tuple{"akey", uint64(2)}
		got := db.bt.Get(wantKey)
		if !reflect.DeepEqual(got, keyValue{wantKey, wantValue}) {
			t.Errorf("Set Get: got %v, want %v", got, wantValue)
		}
	})

	t.Run("seqIncrements", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			tx.Set(Key(internal.Tuple{"akey"}.Pack()), []byte("anoldvalue"))
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		wantValue := []byte("avalue")
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			tx.Set(Key(internal.Tuple{"akey"}.Pack()), wantValue)
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		db.bt.Ascend(nil, func(item interface{}) bool {
			t.Logf("Item: %+v", item)
			return true
		})

		if got, want := db.bt.Len(), 2; got != want {
			t.Errorf("Set Len: got %v, want %v", got, want)
		}

		wantKey := internal.Tuple{"akey", uint64(3)}
		got := db.bt.Get(wantKey)
		if !reflect.DeepEqual(got, keyValue{wantKey, wantValue}) {
			t.Errorf("Set Get: got %v, want %v", got, wantValue)
		}
	})

	t.Run("failsWriteTaint", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			tx.Set(Key(internal.Tuple{"akey"}.Pack()), []byte("avalue"))

			// This will fail to commit because akey is already tainted.
			_, err := db.Transact(func(tx Transaction) (interface{}, error) {
				tx.Set(Key(internal.Tuple{"akey"}.Pack()), []byte("anewervalue"))
				return nil, nil
			})
			if !errors.Is(err, RetryableError{}) {
				t.Fatalf("Transact err: got %#v, want RetryableError", err)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact(two) failed: %v", err)
		}

		db.bt.Ascend(nil, func(item interface{}) bool {
			t.Logf("Item: %+v", item)
			return true
		})

		if got, want := db.bt.Len(), 1; got != want {
			t.Errorf("Set Len: got %v, want %v", got, want)
		}
	})
}

func TestTransactionSet(t *testing.T) {
	t.Run("overwrites", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		wantValue := []byte("anewervalue")
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			tx.Set(Key(internal.Tuple{"akey"}.Pack()), []byte("avalue"))
			tx.Set(Key(internal.Tuple{"akey"}.Pack()), wantValue)
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		db.bt.Ascend(nil, func(item interface{}) bool {
			t.Logf("Item: %+v", item)
			return true
		})

		if got, want := db.bt.Len(), 1; got != want {
			t.Errorf("Set Len: got %v, want %v", got, want)
		}

		wantKey := internal.Tuple{"akey", uint64(2)}
		got := db.bt.Get(wantKey)
		if !reflect.DeepEqual(got, keyValue{wantKey, wantValue}) {
			t.Errorf("Set Get: got %v, want %v", got, wantValue)
		}
	})
}

func TestTransactionGetRange(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		var got []KeyValue
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			ri := tx.GetRange(KeyRange{Key{}, Key(internal.Tuple{[]byte{0xFF}}.Pack())}, RangeOptions{}).Iterator()
			for ri.Advance() {
				kv, err := ri.Get()
				if err != nil {
					return nil, err
				}
				got = append(got, kv)
			}

			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		var want []KeyValue
		if !reflect.DeepEqual(got, want) {
			t.Errorf("GetRange: got %v, want %v", got, want)
		}
	})

	t.Run("single", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		wantKey := internal.Tuple{"akey", uint64(2)}
		wantValue := []byte("anewervalue")
		db.bt.Set(keyValue{wantKey, wantValue})
		db.prevSeq = 2

		var got []KeyValue
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			ri := tx.GetRange(KeyRange{Key{}, Key(internal.Tuple{"\xFF"}.Pack())}, RangeOptions{}).Iterator()
			for ri.Advance() {
				kv, err := ri.Get()
				if err != nil {
					return nil, err
				}
				got = append(got, kv)
			}

			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		want := []KeyValue{
			{wantKey[:1].Pack(), wantValue},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("GetRange: got %v, want %v", got, want)
		}
	})

	t.Run("differentTypes", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		db.bt.Set(keyValue{internal.Tuple{"akey", uint64(2)}, []byte("anewervalue")})
		db.prevSeq = 2

		var got []KeyValue
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			// The key in the BTree is a string, but we use an int
			// range here.
			ri := tx.GetRange(KeyRange{Key(internal.Tuple{42}.Pack()), Key(internal.Tuple{43}.Pack())}, RangeOptions{}).Iterator()
			for ri.Advance() {
				kv, err := ri.Get()
				if err != nil {
					return nil, err
				}
				got = append(got, kv)
			}

			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		var want []KeyValue
		if !reflect.DeepEqual(got, want) {
			t.Errorf("GetRange: got %v, want %v", got, want)
		}
	})
}
