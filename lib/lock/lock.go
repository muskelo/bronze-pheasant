package lock

import "errors"

var ErrLockExpired = errors.New("The Lock went bad")

type Lock interface {
	IsFresh() bool
}
