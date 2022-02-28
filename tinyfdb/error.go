package tinyfdb

// A RetryableError is a wrapper for an error the database code
// considers temporary. Usually a conflicting transaction, which means
// that retrying will likely succeed.
type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string {
	return e.Err.Error()
}

func (e RetryableError) Is(err error) bool {
	_, ok := err.(RetryableError)
	return ok
}

func (e RetryableError) Unwrap() error {
	return e.Err
}
