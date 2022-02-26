package internal

import "testing"

func TestAPIVersion(t *testing.T) {
	t.Run("getNotSet", func(t *testing.T) {
		_, err := GetAPIVersion()
		if err == nil {
			t.Fatalf("GetAPIVersion err: got %v, want non-nil", err)
		}
	})

	t.Run("setAndGet", func(t *testing.T) {
		t.Cleanup(func() {
			apiVersion = 0
		})

		want := 700
		if err := APIVersion(want); err != nil {
			t.Fatalf("APIVersion failed: %v", err)
		}

		got, err := GetAPIVersion()
		if err != nil {
			t.Fatalf("GetAPIVersion failed: %v", err)
		}

		if got != want {
			t.Errorf("GetAPIVersion: got %v, want %v", got, want)
		}
	})

	t.Run("setAndSet", func(t *testing.T) {
		t.Cleanup(func() {
			apiVersion = 0
		})

		if err := APIVersion(700); err != nil {
			t.Fatalf("APIVersion failed: %v", err)
		}

		if err := APIVersion(701); err == nil {
			t.Fatalf("APIVersion(701) err: got %v, want non-nil", err)
		}
	})
}
