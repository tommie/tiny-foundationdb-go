package tinyfdb

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/tidwall/btree"
	"github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"
)

func TestRangeResult(t *testing.T) {
	makeKey := func(k byte, seq uint64) internal.Tuple {
		return internal.Tuple{[]byte{k}, seq}
	}
	makeKey2 := func(k byte, seq uint64) []byte {
		return makeKey(k, seq)[:1].Pack()
	}

	tx := fakeRangeResultTransaction{
		Keys: []internal.Tuple{
			makeKey(10, 1),
			makeKey(11, 1),
			makeKey(12, 1),
		},
	}
	rr := newRangeResult(&tx, FirstGreaterOrEqual(Key(nil)), FirstGreaterThan(Key((internal.Tuple{[]byte{0xFF}}).Pack())))
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
	makeKey := func(k byte, seq uint64) internal.Tuple {
		return internal.Tuple{[]byte{k}, seq}
	}
	makeKey2 := func(k byte, seq uint64) internal.Tuple {
		return makeKey(k, seq)[:1]
	}

	tsts := []struct {
		Name  string
		Begin internal.Tuple
		End   internal.Tuple
		Keys  []internal.Tuple

		WantKeys []internal.Tuple
	}{
		{"empty", nil, nil, nil, nil},
		{"all", nil, makeKey2(0xFF, 0xFF), []internal.Tuple{makeKey(10, 1), makeKey(11, 1), makeKey(12, 1)}, []internal.Tuple{makeKey(10, 1), makeKey(11, 1), makeKey(12, 1)}},

		{"skipBegin", makeKey2(11, 1), makeKey2(0xFF, 0xFF), []internal.Tuple{makeKey(10, 1), makeKey(11, 1), makeKey(12, 1)}, []internal.Tuple{makeKey(11, 1), makeKey(12, 1)}},
		{"skipEnd", nil, makeKey2(11, 0xFF), []internal.Tuple{makeKey(10, 1), makeKey(11, 1), makeKey(12, 1)}, []internal.Tuple{makeKey(10, 1)}},

		{"lastSeq", nil, makeKey2(0xFF, 0xFF), []internal.Tuple{makeKey(10, 1), makeKey(10, 2), makeKey(11, 1)}, []internal.Tuple{makeKey(10, 2), makeKey(11, 1)}},
		{"lastSeqEnd", nil, makeKey2(0xFF, 0xFF), []internal.Tuple{makeKey(10, 1), makeKey(10, 2)}, []internal.Tuple{makeKey(10, 2)}},
	}
	for _, tst := range tsts {
		t.Run(tst.Name, func(t *testing.T) {
			tx := fakeRangeResultTransaction{
				Keys: tst.Keys,
			}
			ri := RangeIterator{
				rr: RangeResult{
					t:   &tx,
					seq: 5,
				},
				next: keyMatcher{sel: firstGreaterOrEqual(tst.Begin)},
				end:  keyMatcher{sel: firstGreaterOrEqual(tst.End)},
			}

			var gotValues []string
			for ri.Advance() {
				kv, _ := ri.Get()
				gotValues = append(gotValues, string(kv.Value))
			}

			var wantValues []string
			var wantTaints []internal.Tuple
			for _, k := range tst.WantKeys {
				wantValues = append(wantValues, fmt.Sprint(k))
				wantTaints = append(wantTaints, k[:len(k)-1])
			}
			if !reflect.DeepEqual(gotValues, wantValues) {
				t.Errorf("Advance: got %+v, want %+v", gotValues, wantValues)
			}

			if !reflect.DeepEqual(tx.GotTaint, wantTaints) {
				t.Errorf("Advance GotTaint: got %+v, want %+v", tx.GotTaint, wantTaints)
			}
		})
	}

	tsts2 := []struct {
		Name   string
		Begin  internal.Tuple
		End    internal.Tuple
		Keys   []internal.Tuple
		Values [][]byte

		WantKeys []internal.Tuple
	}{
		{"earlierTombstone", nil, makeKey2(0xFF, 0xFF), []internal.Tuple{makeKey(10, 1), makeKey(10, 2)}, [][]byte{nil, []byte(fmt.Sprint(makeKey(10, 2)))}, []internal.Tuple{makeKey(10, 2)}},
		{"latestTombstone", nil, makeKey2(0xFF, 0xFF), []internal.Tuple{makeKey(10, 1), makeKey(10, 2), makeKey(11, 1)}, [][]byte{{42}, nil, []byte(fmt.Sprint(makeKey(11, 1)))}, []internal.Tuple{makeKey(11, 1)}},
	}
	for _, tst := range tsts2 {
		t.Run(tst.Name, func(t *testing.T) {
			tx := fakeRangeResultTransaction{
				Keys:  tst.Keys,
				Value: func(i int) []byte { return tst.Values[i] },
			}
			ri := RangeIterator{
				rr: RangeResult{
					t:   &tx,
					seq: 5,
				},
				next: keyMatcher{sel: firstGreaterOrEqual(tst.Begin)},
				end:  keyMatcher{sel: firstGreaterOrEqual(tst.End)},
			}

			var gotValues []string
			for ri.Advance() {
				kv, _ := ri.Get()
				gotValues = append(gotValues, string(kv.Value))
			}

			var wantValues []string
			var wantTaints []internal.Tuple
			for _, k := range tst.WantKeys {
				wantValues = append(wantValues, fmt.Sprint(k))
				wantTaints = append(wantTaints, k[:len(k)-1])
			}
			if !reflect.DeepEqual(gotValues, wantValues) {
				t.Errorf("Advance: got %+v, want %+v", gotValues, wantValues)
			}

			if !reflect.DeepEqual(tx.GotTaint, wantTaints) {
				t.Errorf("Advance GotTaint: got %+v, want %+v", tx.GotTaint, wantTaints)
			}
		})
	}
}

type fakeRangeResultTransaction struct {
	Keys     []internal.Tuple
	Value    func(int) []byte
	GotTaint []internal.Tuple

	bt *btree.BTree
}

func (t *fakeRangeResultTransaction) ascend(pivot internal.Tuple, fun func(keyValue) bool) {
	if t.bt == nil {
		t.bt = btree.NewNonConcurrent(btreeBefore)
		for i, key := range t.Keys {
			v := []byte(fmt.Sprint(key))
			if t.Value != nil {
				v = t.Value(i)
			}
			t.bt.Set(keyValue{key, v})
		}
	}

	t.bt.Ascend(pivot, func(item interface{}) bool {
		return fun(item.(keyValue))
	})
}

func (t *fakeRangeResultTransaction) setTaint(key internal.Tuple, typ taintType) {
	t.GotTaint = append(t.GotTaint, key[:1])
}

func TestKeyMatcher(t *testing.T) {
	var (
		emptyKey internal.Tuple = nil
		aKey                    = internal.Tuple{[]byte{0}}
	)

	tsts := []struct {
		Name   string
		Sel    keySelector
		KeySeq []internal.Tuple
		Want   int
	}{
		{"lastLessThanEmpty", lastLessThan(emptyKey), nil, -1},
		{"lastLessThanNoMatch", lastLessThan(emptyKey), []internal.Tuple{emptyKey}, -1},
		{"lastLessThanMatchLast", lastLessThan(aKey), []internal.Tuple{emptyKey}, 0},
		{"lastLessThanMatch", lastLessThan(aKey), []internal.Tuple{emptyKey, aKey}, 0},
		{"lastLessThanMatchEqual", lastLessThan(aKey), []internal.Tuple{emptyKey, emptyKey, aKey}, 1},

		{"lastLessOrEqualEmpty", lastLessOrEqual(emptyKey), nil, -1},
		{"lastLessOrEqualNoMatch", lastLessOrEqual(emptyKey), []internal.Tuple{aKey}, -1},
		{"lastLessOrEqualMatchLast", lastLessOrEqual(aKey), []internal.Tuple{aKey}, 0},
		{"lastLessOrEqualMatch", lastLessOrEqual(emptyKey), []internal.Tuple{emptyKey, aKey}, 0},
		{"lastLessOrEqualMatchEqual", lastLessOrEqual(emptyKey), []internal.Tuple{emptyKey, emptyKey, aKey}, 1},

		{"firstGreaterThanEmpty", firstGreaterThan(emptyKey), nil, -1},
		{"firstGreaterThanNoMatch", firstGreaterThan(emptyKey), []internal.Tuple{emptyKey}, -1},
		{"firstGreaterThanMatchLast", firstGreaterThan(emptyKey), []internal.Tuple{aKey}, 0},
		{"firstGreaterThanMatch", firstGreaterThan(emptyKey), []internal.Tuple{emptyKey, aKey}, 1},
		{"firstGreaterThanMatchEqual", firstGreaterThan(emptyKey), []internal.Tuple{emptyKey, emptyKey, aKey}, 2},

		{"firstGreaterOrEqualEmpty", firstGreaterOrEqual(emptyKey), nil, -1},
		{"firstGreaterOrEqualNoMatch", firstGreaterOrEqual(aKey), []internal.Tuple{emptyKey}, -1},
		{"firstGreaterOrEqualMatchLast", firstGreaterOrEqual(emptyKey), []internal.Tuple{emptyKey}, 0},
		{"firstGreaterOrEqualMatch", firstGreaterOrEqual(aKey), []internal.Tuple{emptyKey, aKey}, 1},
		{"firstGreaterOrEqualMatchEqual", firstGreaterOrEqual(aKey), []internal.Tuple{emptyKey, aKey, aKey}, 1},
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

func lastLessThan(key internal.Tuple) keySelector     { return keySelector{key, false, 0} }
func lastLessOrEqual(key internal.Tuple) keySelector  { return keySelector{key, true, 0} }
func firstGreaterThan(key internal.Tuple) keySelector { return keySelector{key, true, 1} }
