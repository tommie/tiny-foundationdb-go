package internal

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"
)

// Cmp compares this tuple to another and returns the comparison-int
// corresponding to the theoretical `a - b`.
func (a Tuple) Cmp(b Tuple) (int, error) {
	return compareTuple(a, b)
}

func compareTuple(a, b TupleElementer) (int, error) {
	aa := a.TupleElements()
	bb := b.TupleElements()
	n := len(aa)
	if n > len(bb) {
		n = len(bb)
	}

	for i := 0; i < n; i++ {
		v, err := compareTupleElement(aa[i], bb[i])
		if err != nil {
			return 0, fmt.Errorf("element %d: %w", i, err)
		}
		if v != 0 {
			return v, nil
		}
	}

	return len(aa) - len(bb), nil
}

func compareTupleElement(a, b TupleElement) (int, error) {
	switch aa := a.(type) {
	case TupleElementer:
		if bb, ok := b.(TupleElementer); ok {
			return compareTuple(aa, bb)
		}
		// We already know b isn't a tuple, so there's no
		// recursion here.
		return compareTupleElementType(a, b)
	case nil:
		if b == nil {
			return 0, nil
		}
		return -1, nil
	case int:
		if bb, ok := b.(int); ok {
			return compareInt64(int64(aa), int64(bb)), nil
		}
		if bb, ok := b.(int64); ok {
			return compareInt64(int64(aa), bb), nil
		}
		return compareTupleElementType(a, b)
	case int64:
		if bb, ok := b.(int); ok {
			return compareInt64(aa, int64(bb)), nil
		}
		if bb, ok := b.(int64); ok {
			return compareInt64(aa, bb), nil
		}
		return compareTupleElementType(a, b)
	case uint:
		if bb, ok := b.(uint); ok {
			return compareUint64(uint64(aa), uint64(bb)), nil
		}
		if bb, ok := b.(uint64); ok {
			return compareUint64(uint64(aa), bb), nil
		}
		return compareTupleElementType(a, b)
	case uint64:
		if bb, ok := b.(uint); ok {
			return compareUint64(aa, uint64(bb)), nil
		}
		if bb, ok := b.(uint64); ok {
			return compareUint64(aa, bb), nil
		}
		return compareTupleElementType(a, b)
	case *big.Int:
		if bb, ok := b.(*big.Int); ok {
			return aa.Cmp(bb), nil
		}
		if bb, ok := b.(big.Int); ok {
			return aa.Cmp(&bb), nil
		}
		return compareTupleElementType(a, b)
	case big.Int:
		if bb, ok := b.(*big.Int); ok {
			return aa.Cmp(bb), nil
		}
		if bb, ok := b.(big.Int); ok {
			return aa.Cmp(&bb), nil
		}
		return compareTupleElementType(a, b)
	case []byte:
		if bb, ok := b.([]byte); ok {
			return bytes.Compare(aa, bb), nil
		}
		if bb, ok := b.(KeyConvertible); ok {
			return bytes.Compare(aa, bb.FDBKey()), nil
		}
		return compareTupleElementType(a, b)
	case KeyConvertible:
		if bb, ok := b.([]byte); ok {
			return bytes.Compare(aa.FDBKey(), bb), nil
		}
		if bb, ok := b.(KeyConvertible); ok {
			return bytes.Compare(aa.FDBKey(), bb.FDBKey()), nil
		}
		return compareTupleElementType(a, b)
	case string:
		if bb, ok := b.(string); ok {
			return strings.Compare(aa, bb), nil
		}
		return compareTupleElementType(a, b)
	case float32:
		bb, ok := b.(float32)
		if !ok {
			return compareTupleElementType(a, b)
		}
		if aa < bb {
			return -1, nil
		} else if aa > bb {
			return 1, nil
		}
		return 0, nil
	case float64:
		bb, ok := b.(float64)
		if !ok {
			return compareTupleElementType(a, b)
		}
		if aa < bb {
			return -1, nil
		} else if aa > bb {
			return 1, nil
		}
		return 0, nil
	case bool:
		bb, ok := b.(bool)
		if !ok {
			return compareTupleElementType(a, b)
		}
		if !aa && bb {
			return -1, nil
		} else if aa && !bb {
			return 1, nil
		}
		return 0, nil
	case UUID:
		bb, ok := b.(UUID)
		if !ok {
			return compareTupleElementType(a, b)
		}
		return bytes.Compare(aa[:], bb[:]), nil
	case Versionstamp:
		bb, ok := b.(Versionstamp)
		if !ok {
			return compareTupleElementType(a, b)
		}
		if v := bytes.Compare(aa.TransactionVersion[:], bb.TransactionVersion[:]); v != 0 {
			return v, nil
		}
		if aa.UserVersion < bb.UserVersion {
			return -1, nil
		} else if aa.UserVersion > bb.UserVersion {
			return 1, nil
		}
		return 0, nil

	default:
		return 0, fmt.Errorf("uncomparable types: %T and %T", a, b)
	}
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

func compareUint64(a, b uint64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// compareTupleElementType gives a comparison-int by looking only at
// the type of element. This is used when `a` and `b` have different
// types.
func compareTupleElementType(a, b TupleElement) (int, error) {
	aa, err := elementTypeOrder(a)
	if err != nil {
		return 0, err
	}
	bb, err := elementTypeOrder(b)
	if err != nil {
		return 0, err
	}
	return aa - bb, nil
}

// elementTypeOrder defines an ordering for the types of
// elements. This means that all (int, int) tuples come before all
// (int, int64) tuples and so on.
//
// The ordering isn't important, as long as types that are
// indistinguishable in packed form have the same order
// value. compareTupleElement must be able to compare any combination
// of types that have the same order value.
func elementTypeOrder(e TupleElement) (int, error) {
	switch e.(type) {
	case TupleElementer:
		return 1, nil
	case nil:
		return 2, nil
	case int:
		return 3, nil
	case int64:
		return 3, nil
	case uint:
		return 5, nil
	case uint64:
		return 5, nil
	case *big.Int:
		return 7, nil
	case big.Int:
		return 7, nil
	case []byte:
		return 9, nil
	case KeyConvertible:
		return 9, nil
	case string:
		return 11, nil
	case float32:
		return 12, nil
	case float64:
		return 13, nil
	case bool:
		return 14, nil
	case UUID:
		return 15, nil
	case Versionstamp:
		return 16, nil
	default:
		return 0, fmt.Errorf("uncomparable type: %T", e)
	}
}
