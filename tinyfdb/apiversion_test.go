package tinyfdb

import (
	"testing"

	"github.com/tommie/tiny-foundationdb-go/tinyfdb/internal"
)

func TestAPIVersion(t *testing.T) {
	t.Run("isNotSet", func(t *testing.T) {
		if IsAPIVersionSelected() {
			t.Fatalf("IsAPIVersionSelected: got true, want false")
		}
	})

	t.Run("setAndIs", func(t *testing.T) {
		t.Cleanup(internal.ClearAPIVersion)

		want := 200
		if err := APIVersion(want); err != nil {
			t.Fatalf("APIVersion failed: %v", err)
		}

		if !IsAPIVersionSelected() {
			t.Fatalf("IsAPIVersionSelected: got false, want true")
		}

		got, err := GetAPIVersion()
		if err != nil {
			t.Fatalf("GetAPIVersion failed: %v", err)
		}

		if got != want {
			t.Errorf("GetAPIVersion: got %v, want %v", got, want)
		}
	})
}
