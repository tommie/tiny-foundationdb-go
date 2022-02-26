package tinyfdb

import (
	"github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"
)

// APIVersion sets the version of the API the calling code supports.
func APIVersion(version int) error {
	return internal.APIVersion(version)
}

func GetAPIVersion() (int, error) {
	return internal.GetAPIVersion()
}

func IsAPIVersionSelected() bool {
	_, err := GetAPIVersion()
	return err == nil
}

func MustAPIVersion(version int) {
	if err := internal.APIVersion(version); err != nil {
		panic(err)
	}
}

func MustGetAPIVersion() int {
	v, err := internal.GetAPIVersion()
	if err != nil {
		panic(err)
	}
	return v
}
