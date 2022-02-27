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

func TestTransactionClearRange(t *testing.T) {
	db, err := OpenDefault()
	if err != nil {
		t.Fatalf("OpenDefault failed: %v", err)
	}

	db.bt.Set(keyValue{internal.Tuple{1, uint64(1)}, []byte("value1")})
	db.bt.Set(keyValue{internal.Tuple{2, uint64(1)}, []byte("value2")})
	db.bt.Set(keyValue{internal.Tuple{3, uint64(1)}, nil}) // A tombstone.
	db.bt.Set(keyValue{internal.Tuple{4, uint64(1)}, []byte("value3")})
	db.prevSeq = 1

	_, err = db.Transact(func(tx Transaction) (interface{}, error) {
		tx.ClearRange(KeyRange{
			Key(internal.Tuple{2}.Pack()),
			Key(internal.Tuple{4}.Pack()),
		})

		if want := map[string]taintType{string(internal.Tuple{2}.Pack()): writeTaint}; !reflect.DeepEqual(tx.taints, want) {
			t.Errorf("ClearRange taints: got %+v, want %+v", tx.taints, want)
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Transact failed: %v", err)
	}

	db.bt.Ascend(nil, func(item interface{}) bool {
		t.Logf("Item: %+v", item)
		return true
	})

	if got, want := db.bt.Len(), 5; got != want {
		t.Errorf("Set Len: got %v, want %v", got, want)
	}
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

func TestTransactionGet(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		var got []byte
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			fbs := tx.Get(Key{})
			bs, err := fbs.Get()
			if err != nil {
				return nil, err
			}
			got = bs
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		var want []byte
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Get: got %v, want %v", got, want)
		}
	})

	t.Run("found", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		wantKey := internal.Tuple{"akey", uint64(2)}
		wantValue := []byte("anewervalue")
		db.bt.Set(keyValue{internal.Tuple{"akey", uint64(1)}, []byte("avalue")})
		db.bt.Set(keyValue{wantKey, wantValue})
		db.bt.Set(keyValue{internal.Tuple{"akey", uint64(3)}, []byte("anewestvalue")})
		db.prevSeq = 2

		var got []byte
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			fbs := tx.Get(Key(wantKey[:1].Pack()))
			bs, err := fbs.Get()
			if err != nil {
				return nil, err
			}
			got = bs

			if want := map[string]taintType{string(wantKey.Pack()): readTaint}; !reflect.DeepEqual(tx.taints, want) {
				t.Errorf("Get taints: got %+v, want %+v", tx.taints, want)
			}

			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		if !reflect.DeepEqual(got, wantValue) {
			t.Errorf("Get: got %v, want %v", got, wantValue)
		}
	})

	t.Run("notFound", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		db.bt.Set(keyValue{internal.Tuple{"akey", uint64(1)}, []byte("avalue")})

		var got []byte
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			fbs := tx.Get(Key(internal.Tuple{"anotherkey"}.Pack()))
			bs, err := fbs.Get()
			if err != nil {
				return nil, err
			}
			got = bs

			if want := map[string]taintType{}; !reflect.DeepEqual(tx.taints, want) {
				t.Errorf("Get taints: got %+v, want %+v", tx.taints, want)
			}

			return nil, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		if got != nil {
			t.Errorf("Get: got %v, want nil", got)
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

func TestTransactionAscend(t *testing.T) {
	makeKey := func(k byte, seq uint64) internal.Tuple {
		return internal.Tuple{[]byte{k}, seq}
	}
	makeKey2 := func(k byte, seq uint64) internal.Tuple {
		return makeKey(k, seq)[:1]
	}

	tsts := []struct {
		Name  string
		Pivot internal.Tuple
		Keys  []internal.Tuple
		Seq   uint64

		WantKeys []internal.Tuple
	}{
		{"seqNoMatch", makeKey2(10, 0), []internal.Tuple{makeKey(10, 2)}, 1, nil},
		{"seqLatest", makeKey2(10, 0), []internal.Tuple{makeKey(10, 1), makeKey(10, 2), makeKey(10, 3)}, 2, []internal.Tuple{makeKey(10, 1), makeKey(10, 2)}},
		{"seqIsolated", makeKey2(10, 0), []internal.Tuple{makeKey(10, 1), makeKey(10, 2), makeKey(11, 1)}, 2, []internal.Tuple{makeKey(10, 1), makeKey(10, 2), makeKey(11, 1)}},
	}
	for _, tst := range tsts {
		t.Run(tst.Name, func(t *testing.T) {
			db, err := OpenDefault()
			if err != nil {
				t.Fatalf("OpenDefault failed: %v", err)
			}

			for _, k := range tst.Keys {
				db.bt.Set(keyValue{k, []byte("anewervalue")})
			}
			db.prevSeq = tst.Seq

			var got []internal.Tuple
			_, err = db.Transact(func(tx Transaction) (interface{}, error) {
				tx.ascend(tst.Pivot, func(kv keyValue) bool {
					got = append(got, kv.Key)
					return true
				})

				return nil, nil
			})
			if err != nil {
				t.Fatalf("Transact failed: %v", err)
			}

			if !reflect.DeepEqual(got, tst.WantKeys) {
				t.Errorf("ascend: got %+v, want %+v", got, tst.WantKeys)
			}
		})
	}
}
