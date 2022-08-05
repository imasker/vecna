package tasks

import (
	"errors"
	"fmt"
	"time"
)

// ErrRetryTaskLater ...
type ErrRetryTaskLater struct {
	msg     string
	retryIn time.Duration
}

// RetryIn returns time.Duration from now when task should be retried
func (e ErrRetryTaskLater) RetryIn() time.Duration {
	return e.retryIn
}

// Error implements the error interface
func (e ErrRetryTaskLater) Error() string {
	return fmt.Sprintf("task error: %s, will retry in: %s", e.msg, e.retryIn)
}

// NewErrRetryTaskLater return a new ErrRetryTaskLater instance
func NewErrRetryTaskLater(msg string, retryIn time.Duration) ErrRetryTaskLater {
	return ErrRetryTaskLater{msg: msg, retryIn: retryIn}
}

// Retryable is interface that retriable errors should implement
type Retryable interface {
	RetryIn() time.Duration
}

// ErrCodeInconsistent indicates that the tasks in the same group or chord has different code which should be consistent
var ErrCodeInconsistent = errors.New("tasks should have same code")

// ErrNoTasks indicates that there are not even one task in the group/chord/chain
var ErrNoTasks = errors.New("at least one task")
