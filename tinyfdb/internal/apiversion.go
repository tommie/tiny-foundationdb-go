package internal

import (
	"fmt"
	"sync/atomic"
)

var apiVersion int32

func APIVersion(version int) error {
	if version < 200 || version >= 300 {
		return fmt.Errorf("version not supported: %d", version)
	}
	if !atomic.CompareAndSwapInt32(&apiVersion, 0, int32(version)) {
		return fmt.Errorf("version already set: got %d, had %d", version, atomic.LoadInt32(&apiVersion))
	}
	return nil
}

func GetAPIVersion() (int, error) {
	v := atomic.LoadInt32(&apiVersion)
	if v == 0 {
		return 0, fmt.Errorf("the API version hasn't been selected using tinyfdb.APIVersion")
	}
	return int(v), nil
}

func ClearAPIVersion() {
	atomic.StoreInt32(&apiVersion, 0)
}
