package internal

type Key []byte

func (k Key) FDBKey() Key { return k }

type KeyConvertible interface {
	FDBKey() Key
}
