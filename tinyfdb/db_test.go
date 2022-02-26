package tinyfdb

import (
	"errors"
	"testing"
)

func TestOpenDefault(t *testing.T) {
	_, err := OpenDefault()
	if err != nil {
		t.Fatalf("OpenDefault failed: %v", err)
	}
}

func TestDatabaseCreateTransaction(t *testing.T) {
	db, err := OpenDefault()
	if err != nil {
		t.Fatalf("OpenDefault failed: %v", err)
	}

	tx, err := db.CreateTransaction()
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	if _, ok := db.database.txmap[tx.transaction]; !ok {
		t.Errorf("txmap: transaction not registered: %v", db.database.txmap)
	}

	tx.Cancel()

	if _, ok := db.database.txmap[tx.transaction]; ok {
		t.Errorf("txmap: transaction not unregistered: %v", db.database.txmap)
	}
}

func TestDatabaseTransact(t *testing.T) {
	t.Run("returnsValue", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		want := "hello world"
		got, err := db.Transact(func(tx Transaction) (interface{}, error) {
			return want, nil
		})
		if err != nil {
			t.Fatalf("Transact failed: %v", err)
		}

		if got != want {
			t.Errorf("Transact: got %q, want %q", got, want)
		}
	})

	t.Run("failNoRetry", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		var i int
		wantErr := errors.New("mocked error")
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			i++
			return nil, wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("Transact err: got %v, want %v", err, wantErr)
		}

		if want := 1; i != want {
			t.Errorf("Transact i: got %v, want %v", i, want)
		}
	})

	t.Run("failWithRetry", func(t *testing.T) {
		db, err := OpenDefault()
		if err != nil {
			t.Fatalf("OpenDefault failed: %v", err)
		}

		var i int
		wantErr := RetryableError{errors.New("mocked error")}
		_, err = db.Transact(func(tx Transaction) (interface{}, error) {
			i++
			return nil, wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("Transact err: got %v, want %v", err, wantErr)
		}

		if want := maxTransactRetries; i != want {
			t.Errorf("Transact i: got %v, want %v", i, want)
		}
	})
}
