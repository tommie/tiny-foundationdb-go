package tinyfdb

import "errors"

type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string {
	return e.Err.Error()
}

func (e RetryableError) Is(err error) bool {
	eerr, ok := err.(RetryableError)
	if !ok {
		return false
	}

	return eerr.Err == nil || errors.Is(e.Err, eerr.Err)
}

func (e RetryableError) Unwrap() error {
	return e.Err
}
