package tinyfdb

import (
	"reflect"
	"testing"

	"github.com/tidwall/btree"
)

func TestRangeResult(t *testing.T) {
	makeKey := func(k byte, seq byte) [][]byte {
		return [][]byte{{k}, {0, 0, 0, 0, 0, 0, 0, seq}}
	}
	makeKey2 := func(k byte, seq byte) []byte {
		return []byte{k}
	}

	tx := fakeRangeResultTransaction{
		Keys: [][][]byte{
			makeKey(10, 1),
			makeKey(11, 1),
			makeKey(12, 1),
		},
	}
	rr := newRangeResult(&tx, 42, FirstGreaterOrEqual(Key(nil)), LastLessThan(Key{0xFF}))
	ri := rr.Iterator()

	var got [][]byte
	for ri.Advance() {
		kv, _ := ri.Get()
		got = append(got, kv.Key)
	}

	want := [][]byte{
		makeKey2(10, 1),
		makeKey2(11, 1),
		makeKey2(12, 1),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Advance: got %+v, want %+v", got, want)
	}
}

func TestRangeIterator(t *testing.T) {
	makeKey := func(k byte, seq byte) [][]byte {
		return [][]byte{{k}, {0, 0, 0, 0, 0, 0, 0, seq}}
	}
	makeKey2 := func(k byte, seq byte) [][]byte {
		return [][]byte{{k}}
	}

	tsts := []struct {
		Name  string
		Begin [][]byte
		End   [][]byte
		Keys  [][][]byte

		WantKeys [][][]byte
	}{
		{"empty", nil, nil, nil, nil},
		{"all", nil, makeKey2(0xFF, 0xFF), [][][]byte{makeKey(10, 1), makeKey(11, 1), makeKey(12, 1)}, [][][]byte{makeKey2(10, 1), makeKey2(11, 1), makeKey2(12, 1)}},

		{"skipBegin", makeKey2(11, 1), makeKey2(0xFF, 0xFF), [][][]byte{makeKey(10, 1), makeKey(11, 1), makeKey(12, 1)}, [][][]byte{makeKey2(11, 1), makeKey2(12, 1)}},
		{"skipEnd", nil, makeKey2(11, 0xFF), [][][]byte{makeKey(10, 1), makeKey(11, 1), makeKey(12, 1)}, [][][]byte{makeKey2(10, 1)}},

		{"seqNoMatch", nil, makeKey2(0xFF, 0xFF), [][][]byte{makeKey(10, 20), makeKey(11, 20), makeKey(12, 20)}, nil},
		{"seqLatest", nil, makeKey2(0xFF, 0xFF), [][][]byte{makeKey(10, 4), makeKey(10, 5), makeKey(10, 6)}, [][][]byte{makeKey2(10, 5)}},
	}
	for _, tst := range tsts {
		t.Run(tst.Name, func(t *testing.T) {
			tx := fakeRangeResultTransaction{
				Keys: tst.Keys,
			}
			ri := RangeIterator{
				rr: RangeResult{
					t:     &tx,
					seqBS: []byte{0, 0, 0, 0, 0, 0, 0, 5},
				},
				next: keyMatcher{sel: firstGreaterOrEqual(tst.Begin)},
				end:  keyMatcher{sel: firstGreaterOrEqual(tst.End)},
			}

			var got [][][]byte
			for ri.Advance() {
				kv, _ := ri.Get()
				got = append(got, [][]byte{kv.Key})
			}

			if !reflect.DeepEqual(got, tst.WantKeys) {
				t.Errorf("Advance: got %+v, want %+v", got, tst.WantKeys)
			}

			if !reflect.DeepEqual(tx.GotTaint, tst.WantKeys) {
				t.Errorf("Advance GotTaint: got %+v, want %+v", tx.GotTaint, tst.WantKeys)
			}
		})
	}
}

type fakeRangeResultTransaction struct {
	Keys     [][][]byte
	GotTaint [][][]byte

	bt *btree.BTree
}

func (t *fakeRangeResultTransaction) ascend(pivot [][]byte, fun func(keyValue) bool) {
	if t.bt == nil {
		t.bt = btree.NewNonConcurrent(btreeBefore)
		for _, key := range t.Keys {
			t.bt.Set(keyValue{key, []byte("value")})
		}
	}

	t.bt.Ascend(pivot, func(item interface{}) bool {
		return fun(item.(keyValue))
	})
}

func (t *fakeRangeResultTransaction) setTaint(key [][]byte, typ taintType) {
	t.GotTaint = append(t.GotTaint, key[:1])
}

func TestKeyMatcher(t *testing.T) {
	var (
		emptyKey [][]byte = nil
		aKey              = [][]byte{{0}}
	)

	tsts := []struct {
		Name   string
		Sel    keySelector
		KeySeq [][][]byte
		Want   int
	}{
		{"lastLessThanEmpty", lastLessThan(emptyKey), nil, -1},
		{"lastLessThanNoMatch", lastLessThan(emptyKey), [][][]byte{emptyKey}, -1},
		{"lastLessThanMatchLast", lastLessThan(aKey), [][][]byte{emptyKey}, 0},
		{"lastLessThanMatch", lastLessThan(aKey), [][][]byte{emptyKey, aKey}, 0},
		{"lastLessThanMatchEqual", lastLessThan(aKey), [][][]byte{emptyKey, emptyKey, aKey}, 1},

		{"lastLessOrEqualEmpty", lastLessOrEqual(emptyKey), nil, -1},
		{"lastLessOrEqualNoMatch", lastLessOrEqual(emptyKey), [][][]byte{aKey}, -1},
		{"lastLessOrEqualMatchLast", lastLessOrEqual(aKey), [][][]byte{aKey}, 0},
		{"lastLessOrEqualMatch", lastLessOrEqual(emptyKey), [][][]byte{emptyKey, aKey}, 0},
		{"lastLessOrEqualMatchEqual", lastLessOrEqual(emptyKey), [][][]byte{emptyKey, emptyKey, aKey}, 1},

		{"firstGreaterThanEmpty", firstGreaterThan(emptyKey), nil, -1},
		{"firstGreaterThanNoMatch", firstGreaterThan(emptyKey), [][][]byte{emptyKey}, -1},
		{"firstGreaterThanMatchLast", firstGreaterThan(emptyKey), [][][]byte{aKey}, 0},
		{"firstGreaterThanMatch", firstGreaterThan(emptyKey), [][][]byte{emptyKey, aKey}, 1},
		{"firstGreaterThanMatchEqual", firstGreaterThan(emptyKey), [][][]byte{emptyKey, emptyKey, aKey}, 2},

		{"firstGreaterOrEqualEmpty", firstGreaterOrEqual(emptyKey), nil, -1},
		{"firstGreaterOrEqualNoMatch", firstGreaterOrEqual(aKey), [][][]byte{emptyKey}, -1},
		{"firstGreaterOrEqualMatchLast", firstGreaterOrEqual(emptyKey), [][][]byte{emptyKey}, 0},
		{"firstGreaterOrEqualMatch", firstGreaterOrEqual(aKey), [][][]byte{emptyKey, aKey}, 1},
		{"firstGreaterOrEqualMatchEqual", firstGreaterOrEqual(aKey), [][][]byte{emptyKey, aKey, aKey}, 1},
	}
	for _, tst := range tsts {
		t.Run(tst.Name, func(t *testing.T) {
			m := keyMatcher{sel: tst.Sel}
			got := -1
		loop:
			for i, key := range tst.KeySeq {
				v := m.Match(key)
				t.Logf("Match(%+v): %v, hasPrev:%v i:%v", key, v, m.hasPrev, m.i)
				switch v {
				case matchPrev:
					got = i - 1
					break loop

				case matchCurrent:
					got = i
					break loop
				}
			}
			if got == -1 {
				v := m.End()
				t.Logf("End: %v", v)
				switch v {
				case matchPrev:
					got = len(tst.KeySeq) - 1

				case matchCurrent:
					t.Fatal("End returned matchCurrent")
				}
			}
			if got != tst.Want {
				t.Fatalf("Match(%+v, %+v): got %v, want %v", tst.Sel, tst.KeySeq, got, tst.Want)
			}
		})
	}
}

func lastLessThan(key [][]byte) keySelector     { return keySelector{key, false, 0} }
func lastLessOrEqual(key [][]byte) keySelector  { return keySelector{key, true, 0} }
func firstGreaterThan(key [][]byte) keySelector { return keySelector{key, true, 1} }
