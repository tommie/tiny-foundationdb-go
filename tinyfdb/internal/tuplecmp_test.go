package internal

import (
	"math"
	"math/big"
	"testing"
)

func TestCompareTuple(t *testing.T) {
	tsts := []struct {
		Name string
		A, B Tuple
		Want int
	}{
		{"empty", nil, nil, 0},
		{"intAndString", Tuple{42}, Tuple{"astring"}, -8},

		{"nestedEqual", Tuple{Tuple{42}}, Tuple{Tuple{42}}, 0},
		{"nested", Tuple{Tuple{42}}, Tuple{Tuple{41}}, 1},

		{"nil", Tuple{nil}, Tuple{nil}, 0},
		{"nilAndNotNil", Tuple{nil}, Tuple{42}, -1},

		{"intEqual", Tuple{42}, Tuple{42}, 0},
		{"intLess", Tuple{41}, Tuple{42}, -1},
		{"intGreater", Tuple{math.MaxInt}, Tuple{math.MinInt}, 1},

		{"int64Equal", Tuple{int64(42)}, Tuple{int64(42)}, 0},
		{"int64Less", Tuple{int64(math.MinInt64)}, Tuple{int64(math.MaxInt64)}, -1},
		{"int64Greater", Tuple{int64(math.MaxInt64)}, Tuple{int64(math.MinInt64)}, 1},

		{"uintEqual", Tuple{uint(42)}, Tuple{uint(42)}, 0},
		{"uintLess", Tuple{uint(41)}, Tuple{uint(42)}, -1},
		{"uintGreater", Tuple{uint(math.MaxUint)}, Tuple{uint(0)}, 1},

		{"uint64Equal", Tuple{uint64(42)}, Tuple{uint64(42)}, 0},
		{"uint64Less", Tuple{uint64(0)}, Tuple{uint64(math.MaxUint64)}, -1},
		{"uint64Greater", Tuple{uint64(math.MaxUint64)}, Tuple{uint64(0)}, 1},

		{"*bigIntEqual", Tuple{big.NewInt(42)}, Tuple{big.NewInt(42)}, 0},
		{"*bigIntLess", Tuple{big.NewInt(0)}, Tuple{big.NewInt(42)}, -1},

		{"bigIntEqual", Tuple{*big.NewInt(42)}, Tuple{*big.NewInt(42)}, 0},
		{"bigIntLess", Tuple{*big.NewInt(0)}, Tuple{*big.NewInt(42)}, -1},

		{"bytesEqual", Tuple{[]byte{42}}, Tuple{[]byte{42}}, 0},
		{"bytesLess", Tuple{[]byte{41}}, Tuple{[]byte{42}}, -1},

		{"keyEqual", Tuple{Key{42}}, Tuple{Key{42}}, 0},
		{"keyLess", Tuple{Key{41}}, Tuple{Key{42}}, -1},

		{"stringEqual", Tuple{"a"}, Tuple{"a"}, 0},
		{"stringLess", Tuple{"a"}, Tuple{"b"}, -1},

		{"float32Equal", Tuple{float32(42)}, Tuple{float32(42)}, 0},
		{"float32Less", Tuple{float32(-math.MaxFloat32)}, Tuple{float32(math.MaxFloat32)}, -1},
		{"float32Greater", Tuple{float32(math.MaxFloat32)}, Tuple{float32(-math.MaxFloat32)}, 1},

		{"float64Equal", Tuple{float64(42)}, Tuple{float64(42)}, 0},
		{"float64Less", Tuple{float64(-math.MaxFloat64)}, Tuple{float64(math.MaxFloat64)}, -1},
		{"float64Greater", Tuple{float64(math.MaxFloat64)}, Tuple{float64(-math.MaxFloat64)}, 1},

		{"boolEqualTrue", Tuple{true}, Tuple{true}, 0},
		{"boolEqualFalse", Tuple{false}, Tuple{false}, 0},
		{"boolLess", Tuple{false}, Tuple{true}, -1},
		{"boolGreater", Tuple{true}, Tuple{false}, 1},

		{"uuidEqual", Tuple{UUID{42}}, Tuple{UUID{42}}, 0},
		{"uuidLess", Tuple{UUID{41}}, Tuple{UUID{42}}, -1},

		{"versionstampEqual", Tuple{Versionstamp{[10]byte{42}, 100}}, Tuple{Versionstamp{[10]byte{42}, 100}}, 0},
		{"versionstampTxLess", Tuple{Versionstamp{[10]byte{41}, 100}}, Tuple{Versionstamp{[10]byte{42}, 100}}, -1},
		{"versionstampUserLess", Tuple{Versionstamp{[10]byte{42}, 99}}, Tuple{Versionstamp{[10]byte{42}, 100}}, -1},
		{"versionstampUserGreater", Tuple{Versionstamp{[10]byte{42}, 100}}, Tuple{Versionstamp{[10]byte{42}, 99}}, 1},
	}
	for _, tst := range tsts {
		t.Run(tst.Name, func(t *testing.T) {
			got, err := compareTuple(tst.A, tst.B)
			if err != nil {
				t.Fatalf("compareTuple failed: %v", err)
			}

			if got != tst.Want {
				t.Errorf("compareTuple(%v, %v): got %v, want %v", tst.A, tst.B, got, tst.Want)
			}
		})
	}
}
