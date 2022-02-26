package tinyfdb

import (
	"errors"
	"reflect"
	"testing"
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
			tx.Set(Key("akey"), wantValue)
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

		wantKey := [][]byte{[]byte("akey"), {0, 0, 0, 0, 0, 0, 0, 2}}
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
			tx.Set(Key("akey"), []byte("anoldvalue"))
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		wantValue := []byte("avalue")
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			tx.Set(Key("akey"), wantValue)
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

		wantKey := [][]byte{[]byte("akey"), {0, 0, 0, 0, 0, 0, 0, 3}}
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
			tx.Set(Key("akey"), []byte("avalue"))

			// This will fail to commit because akey is already tainted.
			_, err := db.Transact(func(tx Transaction) (interface{}, error) {
				tx.Set(Key("akey"), []byte("anewervalue"))
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
			tx.Set(Key("akey"), []byte("avalue"))
			tx.Set(Key("akey"), wantValue)
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

		wantKey := [][]byte{[]byte("akey"), {0, 0, 0, 0, 0, 0, 0, 2}}
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
			ri := tx.GetRange(KeyRange{Key{}, Key{0xFF}}, RangeOptions{}).Iterator()
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

		wantKey := [][]byte{[]byte("akey"), {0, 0, 0, 0, 0, 0, 0, 2}}
		wantValue := []byte("anewervalue")
		db.bt.Set(keyValue{wantKey, wantValue})
		db.prevSeq = 2

		var got []KeyValue
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			ri := tx.GetRange(KeyRange{Key{}, Key{0xFF}}, RangeOptions{}).Iterator()
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
			{wantKey[0], wantValue},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("GetRange: got %v, want %v", got, want)
		}
	})
}
